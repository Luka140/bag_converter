from rosbags.rosbag2 import Reader
from rosbags.serde import deserialize_cdr

from pathlib import Path

from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore, get_types_from_idl, get_types_from_msg


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

bagpath = Path('/workspaces/brightsky_project/ros_bags/rosbag2_2024-08-12 11:25:08.005992')

# Create reader instance and open for reading.
with AnyReader([bagpath], default_typestore=typestore) as reader:
    connections = [x for x in reader.connections if x.topic == '/timesync']
    for connection, timestamp, rawdata in reader.messages(connections=connections):
         msg = reader.deserialize(rawdata, connection.msgtype)
         print(f'{msg.header1.stamp} - {msg.header2.stamp}' )
         

