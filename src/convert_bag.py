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

def remove_time_offset(array: np.ndarray, offset: float) -> np.ndarray:
    new_array = copy.deepcopy(array)
    new_array[:,0] = new_array[:,0] - offset
    return new_array

def remove_zero_entries(array: np.ndarray) -> np.ndarray:
    new_array = array[np.where(array[:,0] > 1e-6)]
    return new_array

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

# Read definitions to python strings.
msg_paths = [Path('src/stamped_std_msgs/msg/Float32Stamped.msg'),
             Path('src/stamped_std_msgs/msg/Int32Stamped.msg'),
             Path('src/stamped_std_msgs/msg/TimeSync.msg'),
             Path('src/ferrobotics_acf/msg/ACFTelem.msg'),
             Path('src/ferrobotics_acf/msg/ACFTelemStamped.msg')
             ]

# Create a dict for each topic. Entries should be
    # 'parser': a function that parses the msg type and returns a tuple in order (timestamp, d1, d2, d3) where dn are extracted data fields
    # 'column_headers': a list of column header strings for the above datafields (excluding timestamp)
    # 'timetype': either 'plc' or 'rpm' - the clock variation that these messages are timestamped in
    
rpm_dict        = {'parser': process_int_float_stamped,
                   'column_headers': ['rpm'],                               # rpm of the grinder
                   'timetype': 'plc'}                              
acf_force_dict  = {'parser': process_int_float_stamped,
                   'column_headers': ['force_cmd'],                         # The force command
                   'timetype': 'ros'}                     
telem_dict      = {'parser': process_acf_stamped, 
                   'column_headers': ['force', 'position', 'contact_flag'], # Telemetry including true force
                   'timetype': 'ros'} 
timesync_dict   = {'parser': process_timesync}                               # The time offsets between the two clock types

# Top level dict that correlates the topic names with their respective dict
topic_dict      = {'/grinder/rpm': rpm_dict,  
                   '/acf/force': acf_force_dict,    
                   '/acf/telem': telem_dict,          
                   '/timesync': timesync_dict}                             

# Load message types from the standard database and add the custom message types
add_types = {}
[add_types.update(get_types_from_msg(msg_path.read_text(), path_to_type(msg_path))) for msg_path in msg_paths]
typestore = get_typestore(Stores.ROS2_HUMBLE)
typestore.register(add_types)

bagpath = Path('/workspaces/brightsky_project/ros_bags/rosbag2_2024-08-12 13:33:05.918498')
converted_bagpath = Path('csv_bags')
csv_filename = converted_bagpath / f'{bagpath.parts[-1]}.csv'

# Read all messages and parse them according to the 'parcer' in topic_dict
# Stores the messages for each respective topic in an np.ndarray located in topic_dict['topic_name']['array']
with AnyReader([bagpath], default_typestore=typestore) as reader:
    for topic in topic_dict.keys():
        process_func = topic_dict[topic]['parser']
        processed_msgs = [] 
        connections = [x for x in reader.connections if x.topic == topic]
        for connection, timestamp, rawdata in reader.messages(connections=connections):
            msg = reader.deserialize(rawdata, connection.msgtype)
            processed_msgs.append(process_func(msg))
        
        topic_dict[topic]['array'] = np.array(processed_msgs)

# Extract the timestamps from the 'TimeSync' messages
rostime_offset, plctime_offset = topic_dict.pop('/timesync')['array'][0,:]                                        
print(f'ROS time at first match: {rostime_offset}\nPLC time at first match: {plctime_offset}\n')


# Synchronize the timestamps for all topics
for key in topic_dict.keys():
    time_type = topic_dict[key]['timetype']
    if 'plc' in time_type:
        offset = plctime_offset
    else:
        offset = rostime_offset
    
    topic_array = remove_zero_entries(topic_dict[key]['array'])
    topic_dict[key]['array'] = remove_time_offset(topic_array, offset)
    
# Set start time to zero
min_time = min([np.min(topic_dict[topic]['array'][:,0]) for topic in topic_dict.keys()])
for topic in topic_dict.keys():
    topic_dict[topic]['array'] = remove_time_offset(topic_dict[topic]['array'], min_time)

# Extract all unique timestamps
timestamp_sets = [set(topic_dict[topic]['array'][:,0]) for topic in topic_dict.keys()]
unique_timestamp_set = timestamp_sets[0].union(*timestamp_sets[1:])
unique_timestamps = sorted(list(unique_timestamp_set))
print(f"Number of timestamps: {len(unique_timestamps)}")

# Sort keys so that the columns in the csv will stay in the same order on rerun
keys_sorted = sorted([key for key in topic_dict.keys()])

# Dict that stores the latest datapoint for each topic
entries = {key: np.array([None] * (topic_dict[key]['array'].shape[1]-1)) for key in topic_dict.keys()}

with open(csv_filename, 'w') as f:
    # Write headers
    f.write(f'timestamp,{",".join([header for key in keys_sorted for header in topic_dict[key]["column_headers"]])}\n')
    
    # Go through each timestamp and write a line to the csv with the most up to date values for each topic
    for i, timestamp in enumerate(unique_timestamps):
        # Update entries to the most recent datapoint for each topic
        for key in topic_dict.keys():
            timestamp_idx = np.argwhere(topic_dict[key]['array'][:,0] == timestamp)
            
            if len(timestamp_idx) > 0:
                entries[key] = topic_dict[key]['array'][timestamp_idx[0], 1:].flatten()
        
        entry = [timestamp] + [value for key in keys_sorted for value in entries[key]]
        f.write(','.join([str(value) for value in entry]) + '\n')

