from pathlib import Path
import numpy as np
import copy

from rosbags.rosbag2 import Reader
from rosbags.serde import deserialize_cdr

from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore, get_types_from_idl, get_types_from_msg

from stamped_std_msgs.msg import Float32Stamped, Int32Stamped, TimeSync
from ferrobotics_acf.msg import ACFTelemStamped, ACFTelem


def path_to_type(path):
    parts = list(path.parts)
    src_idx = parts.index('src')
    msg_type = ('/'.join(parts[src_idx+1:])).strip(f'{path.suffix}')
    return msg_type
    

# Read definitions to python strings.
msg_paths = [Path('src/stamped_std_msgs/msg/Float32Stamped.msg'),
             Path('src/stamped_std_msgs/msg/Int32Stamped.msg'),
             Path('src/stamped_std_msgs/msg/TimeSync.msg'),
             Path('src/ferrobotics_acf/msg/ACFTelem.msg'),
             Path('src/ferrobotics_acf/msg/ACFTelemStamped.msg')
             ]

# Plain dictionary to hold message definitions.
add_types = {}

# Add definitions from one msg file to the dict.
[add_types.update(get_types_from_msg(msg_path.read_text(), path_to_type(msg_path))) for msg_path in msg_paths]

typestore = get_typestore(Stores.ROS2_HUMBLE)
typestore.register(add_types)

bagpath = Path('/workspaces/brightsky_project/ros_bags/rosbag2_2024-08-12 13:33:05.918498')


# # Create reader instance and open for reading.
# with AnyReader([bagpath], default_typestore=typestore) as reader:
#     connections = [x for x in reader.connections if x.topic == '/grinder/rpm']
#     for connection, timestamp, rawdata in reader.messages(connections=connections):
#         msg = reader.deserialize(rawdata, connection.msgtype)
#         # print(f'{msg.header1.stamp} - {msg.header2.stamp}' )
#         print(f'\n{msg.data} - {msg.header.stamp.sec}')
#         # print(f'msg')


def conv_time_ns(sec, nsec):
    return sec * 10**9 + nsec

def process_int_float_stamped(msg: Float32Stamped | Int32Stamped):
    sec, nsec = msg.header.stamp.sec, msg.header.stamp.nanosec
    return conv_time_ns(sec, nsec), msg.data

def process_acf_stamped(msg: ACFTelemStamped):
    sec, nsec = msg.header.stamp.sec, msg.header.stamp.nanosec
    return conv_time_ns(sec, nsec), msg.telemetry.force, msg.telemetry.position, msg.telemetry.in_contact

def process_timesync(msg: TimeSync):
    t1 = conv_time_ns(msg.header1.stamp.sec, msg.header1.stamp.nanosec) # rostime
    t2 = conv_time_ns(msg.header2.stamp.sec, msg.header2.stamp.nanosec) # plctime
    return t1, t2
    

topics_field = {'/grinder/rpm': process_int_float_stamped,  # rpm of the grinder
                '/acf/force': process_int_float_stamped,    # The force command
                '/acf/telem': process_acf_stamped,          # Telemetry including true force
                '/timesync': process_timesync}              # The time offsets between the two time types


output = {}
with AnyReader([bagpath], default_typestore=typestore) as reader:
    for topic, process_func in topics_field.items():
        processed_msgs = []
        connections = [x for x in reader.connections if x.topic == topic]
        for connection, timestamp, rawdata in reader.messages(connections=connections):
            msg = reader.deserialize(rawdata, connection.msgtype)
            processed_msgs.append(process_func(msg))
        
        output[topic] = processed_msgs

rostime_offset, plctime_offset = output['/timesync'][0]
rpm_array = np.array(output['/grinder/rpm'])
force_cmd_array = np.array(output['/acf/force'])
telem_array = np.array(output['/acf/telem'])


def remove_time_offset(array, offset):
    new_array = copy.deepcopy(array)
    new_array[:,0] = new_array[:,0] - offset
    print(np.min(array[:,0]), np.min(new_array[:,0]), '\n')
    return new_array

# Synchronise clocks
rpm_array =         remove_time_offset(rpm_array, plctime_offset)
force_cmd_array =   remove_time_offset(force_cmd_array, rostime_offset)
telem_array =       remove_time_offset(telem_array, rostime_offset)

# Set start time to zero
min_time = min(np.min(rpm_array[:,0]), np.min(force_cmd_array[:,0]), np.min(telem_array[:,0]))
rpm_array =         remove_time_offset(rpm_array, min_time)
force_cmd_array =   remove_time_offset(force_cmd_array, min_time)
telem_array =       remove_time_offset(telem_array, min_time)

unique_timestamp_set = set(rpm_array[:,0]) | set(force_cmd_array[:,0]) | set(telem_array[:,0])
unique_timestamps = sorted(list(unique_timestamp_set))
# print(f"Number of timestamps: {len(unique_timestamps)}")

# TODO: the shear size of the time values may become an issue
# Format : [nr of timestamps, n] 
data_table = np.zeros((len(unique_timestamps), rpm_array.shape[1]+telem_array.shape[1]+force_cmd_array.shape[1]-3 + 1))

entry_rpm   = np.array([None] * (rpm_array.shape[1] - 1))
entry_force = np.array([None] * (force_cmd_array.shape[1] - 1))
entry_telem = np.array([None] * (telem_array.shape[1] - 1))

# TODO storage location
# TODO timestamps are not correct 
csv_filename = 'table.csv'
with open(csv_filename, 'w') as f:
    for i, timestamp in enumerate(unique_timestamps):
        rpm_entry_idx = np.argwhere(rpm_array[:,0] == timestamp)
        force_entry_idx = np.argwhere(force_cmd_array[:,0] == timestamp)
        telem_entry_idx = np.argwhere(telem_array[:,0] == timestamp)
        
        if len(rpm_entry_idx) > 0:
            entry_rpm = rpm_array[rpm_entry_idx[0], 1:].flatten()
        if len(force_entry_idx) > 0:
            entry_force = force_cmd_array[force_entry_idx[0],1:].flatten()
        if len(telem_entry_idx) > 0:
            entry_telem = telem_array[telem_entry_idx[0],1:].flatten()
        
        entry = (timestamp, *entry_force, *entry_telem, *entry_rpm)
        
        # print(entry_rpm, entry_force, entry_telem)
        # print(entry)
        data_table[i,:] = entry

        f.write(','.join([str(nr) for nr in entry]) + '\n')

print(data_table)
