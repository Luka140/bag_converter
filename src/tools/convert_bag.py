from pathlib import Path
import numpy as np
import copy
from glob import glob 

import open3d as o3d

from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore, get_types_from_msg

from stamped_std_msgs.msg import Float32Stamped, Int32Stamped, TimeSync
from ferrobotics_acf.msg import ACFTelemStamped, ACFTelem
from data_gathering_msgs.msg import BeltWearHistory, GrindArea
from std_msgs.msg import String



"""
The following script processes the rosbag time data into a .csv table which contains for every timestep:
    - Force setpoint
    - Current force
    - flange position
    - RPM
    - flange contact flag (does not seem to work correctly in the ACF driver)
    - failure messages

Values are None until the first timestep a message is received on that topic. 
After that, next rows will always contain the latest received value for that topic. 
    
Required inputs:
    - Path to the file to procees
"""

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
    if 'ros' in msg.header1.frame_id.lower():
        ros_header = msg.header1
        plc_header = msg.header2 
    else:
        ros_header = msg.header2
        plc_header = msg.header1
        
    t_ros = conv_time_ns(ros_header.stamp.sec, ros_header.stamp.nanosec) # rostime
    t_plc = conv_time_ns(plc_header.stamp.sec, plc_header.stamp.nanosec) # plctime
    return t_ros, t_plc

def process_belt_wear_history(msg: BeltWearHistory):
    forces, rpms, contact_times, areas = np.array(msg.force), np.array(msg.rpm), np.array(msg.contact_time), np.array(msg.area)
    return np.sum(forces * contact_times * rpms)

def process_string_msg(msg: String):
    return str(msg.data)

def process_grind_area(msg: GrindArea):
    return msg.belt_width, msg.plate_thickness

def strip_filename_timestamp(name):
    parts = name.split('__')
    settings = parts.pop(-1)
    year_dash_idx = settings.index('-')
    stamp_start_idx = year_dash_idx - 5
    stamp = settings[stamp_start_idx:].strip('_')

    stripped_name = '__'.join(parts + [settings[:stamp_start_idx]])
    return stripped_name, stamp 


def convert_bag(bagpath, precomputed_volume_loss = None, overwrite_area = None, output_folder=None):
    """Converts a rosbag file to a csv.
    This function processes the rosbag time data into a .csv table which contains for every timestep:
        - Force setpoint
        - Current force
        - flange position
        - RPM
        - flange contact flag (does not seem to work correctly in the ACF driver)
        - failure messages

    Args:
        bagpath (Path): Path to the rosbag
        precomputed_volume_loss (list[float], optional): List of precomputed volumes which will overwrite the values in the rosbag messages if applicable.

    """
    # Relative path to the definition of each custom message type 
    msg_paths = [Path('src/stamped_std_msgs/msg/Float32Stamped.msg'),
                Path('src/stamped_std_msgs/msg/Int32Stamped.msg'),
                Path('src/stamped_std_msgs/msg/TimeSync.msg'),
                Path('src/ferrobotics_acf/msg/ACFTelem.msg'),
                Path('src/ferrobotics_acf/msg/ACFTelemStamped.msg'),
                Path('src/data_gathering_msgs/msg/BeltWearHistory.msg')]

    # Create a dict for each topic. Entries should be
        # 'parser': a function that parses the msg type and returns a tuple in order (timestamp, d1, d2, d3) where dn are extracted data fields
        # 'column_headers': a list of n column header strings for the above datafields (excluding timestamp)
        # 'timetype': either 'plc' or 'rpm' - the clock variation that these messages are timestamped in
        
    rpm_dict            = {'parser': process_int_float_stamped,
                        'column_headers': ['rpm'],                               # rpm of the grinder
                        'timetype': 'plc'}                              
    acf_force_dict      = {'parser': process_int_float_stamped,
                        'column_headers': ['force_cmd'],                         # The force command
                        'timetype': 'ros'}                     
    telem_dict          = {'parser': process_acf_stamped, 
                        'column_headers': ['force', 'position', 'contact_flag'], # Telemetry including true force
                        'timetype': 'ros'} 
    timesync_dict       = {'parser': process_timesync}                               # The time offsets between the two clock types
    volume_dict         = {'parser': process_int_float_stamped,
                        'column_headers': ['removed_volume'],
                        'timetype': 'ros'}
    wear_dict           = {'parser': process_belt_wear_history, 'timetype': 'ros'}
    area_dict           = {'parser': process_grind_area,
                           'column_headers': ['grind_area'],
                           'timetype': 'ros'}    
    failure_flag_dict   = {'parser': process_string_msg,
                           'column_headers': ['failure_msg'],
                           'timetype': 'ros'}
    #added topics parsing for moving grinder
    feed_rate_dict   = {'parser': process_int_float_stamped,
                           'column_headers': ['feed_rate'],
                           'timetype': 'ros'}
    num_pass_dict   = {'parser': process_int_float_stamped,
                           'column_headers': ['num_pass'],
                           'timetype': 'ros'}
    pass_length_dict   = {'parser': process_int_float_stamped,
                           'column_headers': ['pass_length'],
                           'timetype': 'ros'}

    # Top level dict that correlates the topic names with their respective dict
    topic_dict      = {'/grinder/rpm': rpm_dict,  
                    '/acf/force': acf_force_dict,    
                    '/acf/telem': telem_dict,          
                    '/timesync': timesync_dict,
                    '/scanner/volume': volume_dict,
                    '/belt_wear_history': wear_dict,
                    '/test_failure': failure_flag_dict,
                    '/grind_area': area_dict,
                    #added topics for moving grinder
                    '/feed_rate': feed_rate_dict,
                    '/num_pass': num_pass_dict,
                    '/pass_length': pass_length_dict}                             

    # Load message types from the standard database and add the custom message types
    add_types = {}
    #[add_types.update(get_types_from_msg(msg_path.read_text(), path_to_type(msg_path))) for msg_path in msg_paths]
    typestore = get_typestore(Stores.ROS2_HUMBLE)
    typestore.register(add_types)
    output_folder = Path('csv_bags')
    
    # Read all messages and parse them according to the 'parser' in topic_dict
    # Stores the messages for each respective topic in an np.ndarray located in topic_dict['topic_name']['array']

    with AnyReader([bagpath], default_typestore=typestore) as reader:
        for topic in topic_dict.keys():
            process_func = topic_dict[topic]['parser']
            # Pre-load an array into the dict in case the topic has no connections 
            topic_dict[topic]['array'] = np.array([])
            processed_msgs = [] 
            connections = [x for x in reader.connections if x.topic == topic]
            
            if len(connections) < 1:
                continue

            for connection, timestamp, rawdata in reader.messages(connections=connections):
                try:
                    msg = reader.deserialize(rawdata, connection.msgtype)
                    processed_msgs.append(process_func(msg))
                except Exception as e:
                    print(f"Error processing topic {topic} at timestamp {timestamp}: {e}")
                    continue
            topic_dict[topic]['array'] = np.array(processed_msgs)

    if topic_dict['/scanner/volume']['array'].size < 1:
        print('Bag does not contain grinded volume --skipping')
        return

    if topic_dict['/belt_wear_history']['array'].size < 1:
        print('Bag does not contain belt wear --not skipping')

    
    if topic_dict['/grind_area']['array'].size < 1 and overwrite_area is None:
        print("The bag does not contain grinded area messages, and overwrite_area is None --skipping")
    
    # Extract the timestamps from the 'TimeSync' messages
    
    rostime_offset, plctime_offset = topic_dict.pop('/timesync')['array'][0,:]                                        
    # print(f'ROS time at first match: {rostime_offset} ns\nPLC time at first match: {plctime_offset} ns\n')

    wear_topic = topic_dict.pop('/belt_wear_history')
    wear = wear_topic['array'][0]


    failure_topic = topic_dict.pop('/test_failure')
    if failure_topic['array'].size > 0:
        failure_msg = '__'.join([failure_topic['array'][i].replace(',', '-').replace('\n', '__') for i in range(failure_topic['array'].size)])
    else:
        failure_msg = ''

    area_topic = topic_dict.pop('/grind_area')
    if area_topic['array'].size > 0:
        # Multiply belt width and thickness converted from meters to mm
        area = area_topic['array'][0,0] * 1000 * area_topic['array'][0,1] * 1000
        # get belt width value in mm
        belt_width = area_topic['array'][0,0] * 1000
    else:
        area = overwrite_area
        belt_width = 25.00                 #currently hard coded

    #Compute factored grind time & material removal
    feed_rate_topic = topic_dict.pop('/feed_rate')
    num_pass_topic = topic_dict.pop('/num_pass')
    pass_length_topic = topic_dict.pop('/pass_length')

    # Synchronize the timestamps for all topics
    for key in topic_dict.keys():
        print(key)
        time_type = topic_dict[key]['timetype']
        if 'plc' in time_type.lower():
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
    print(f"Number of timestamps: {len(unique_timestamps)} - ranging {unique_timestamps[-1]/10**9:.2f} seconds")
    
    # Extract single value messages
    grinded_volume_topic = topic_dict.pop('/scanner/volume')   
    if precomputed_volume_loss is None:   
        grinded_volume = grinded_volume_topic['array'][0, 1]
        print(f'Removed_volume {grinded_volume:.3f}')
    else:
        grinded_volume = precomputed_volume_loss
        print(f'Removed volume precomputed: {grinded_volume:.3f}')

    #Compute Factored time and volume
    fact_grind_time = num_pass_topic['array'][0, 1] * belt_width / feed_rate_topic['array'][0, 1]
    fact_volume = grinded_volume * belt_width / pass_length_topic['array'][0, 1]


    # results = f'th{plate_thickness}_d{removed_material_depth}'
    results = f'v{grinded_volume:.3f}_w{wear:.1f}_a{area:.2f}_bw{belt_width:.2f}_fv{fact_volume:.3f}_ft{fact_grind_time:.2f}'
    filename_stripped, filename_timestamp = strip_filename_timestamp(bagpath.parts[-1])
    csv_filename = output_folder / f'{filename_stripped}_{results}__{filename_timestamp}.csv'

    # similar_result_files = [path for path in converted_bagpath.iterdir() if results in str(path.name)]
    # if len(similar_result_files) > 0:
    #     print(f'\n[WARNING]: another file with plate thickness {plate_thickness} and removed material {removed_material_depth} already exists.') 
    #     # print(f'[WARNING]: Make sure these are the correct numbers for this file.\n')

    # if (plate_thickness is ...) or (removed_material_depth is ...) or (plate_thickness < 1e-9) or (removed_material_depth < 1e-9):
    #     raise TypeError(f'Please insert a valid plate thickness and removed material')


    # Sort keys so that the columns in the csv will stay in the same order on rerun
    keys_sorted = sorted([key for key in topic_dict.keys()])

    # Dict that stores the latest datapoint for each topic
    entries = {key: np.array([None] * (topic_dict[key]['array'].shape[1]-1)) for key in topic_dict.keys()}

    with open(csv_filename, 'w') as f:
        # Write headers
        f.write(f'timestamp,{",".join([header for key in keys_sorted for header in topic_dict[key]["column_headers"]])},failure_msg\n')
        
        # Go through each timestamp and write a line to the csv with the most up to date values for each topic
        for i, timestamp in enumerate(unique_timestamps):
            # Update entries to the most recent datapoint for each topic
            for key in topic_dict.keys():
                timestamp_idx = np.argwhere(topic_dict[key]['array'][:,0] == timestamp)
                
                if len(timestamp_idx) > 0:
                    entries[key] = topic_dict[key]['array'][timestamp_idx[0], 1:].flatten()
            
            entry = [timestamp] + [value for key in keys_sorted for value in entries[key]] + [failure_msg]
            f.write(','.join([str(value) for value in entry]) + '\n')
            
            
def recalculate_volumes(bags: list[Path]) -> list[float]:
    """Recalculates the removed volume from tests using stored pointclouds 

    Args:
        bags (list[Path]): the paths of the rosbags for which the volume should be recalculated.

    Returns:
        list[float]: The recalculated volume differences
    """

    file_identifiers = ['pcl' + strip_filename_timestamp(path.name)[0].strip('rosbag2') for path in bags]
    glob_results = [glob(f'{data_path}/{identifier}*') for identifier in file_identifiers]


    # Copy pasted from the launch file 
    processing_settings = {'dist_threshold':        0.0006,        # Distance to filter grinded area. do not go near #50-80 micron on line axis, 200 micron on feed axis of rate 30 second
                            'cluster_neighbor':      20,           # filter outlier with #of neighbour point threshold
                            'plate_thickness':       0.0023,       # in m
                            'plane_error_allowance': 5,            # in degrees
                            'clusterscan_eps':       0.00025,      # cluster minimum dist grouping in m
                            'laserline_threshold':   0.00008,      # scan resolution line axis in m
                            'feedaxis_threshold':    0.00012,      # scan resolution robot feed axis in m
                            'concave_resolution':    0.0005,       # concave hull resolution
                            'filter_down_size':      0.0002,       # voxel down size on clustering
                            'belt_width_threshold':  0.8,          # minimum detected belt width in m 
        }
    
    pcl_func = PCLfunctions()

    pcl_volumes = []
    lost_volume = None 
    for glob_result in glob_results:
        if len(glob_result) > 0:
            pcl_folder = glob_result[0]
            pcl1_path, pcl2_path = Path(pcl_folder) / 'pre_grind_pointcloud.ply', Path(pcl_folder) / 'post_grind_pointcloud.ply'
            pcl1, pcl2 = o3d.io.read_point_cloud(str(pcl1_path)), o3d.io.read_point_cloud(str(pcl2_path))
            result = pcl_func.calculate_volume_difference(pcl1, pcl2, plate_thickness=2/1000, settings=processing_settings, logger=None)
            if result is not None:
                lost_volume, changed_pcl_global, hull_cloud_global = result 
                # Convert to mm3
                lost_volume *= 1000**3
            else:
                lost_volume = 0.0
        else:
            lost_volume = None 
        
        print(f'\n\nLost volume for bag {glob_result}: \n{lost_volume}\n')
        pcl_volumes.append(lost_volume)
    
    return pcl_volumes
        

if __name__ == '__main__':

    # Flag to recalculate lost volumes with the current iteration of the volume calculation
    # The stored pointclouds are used and re-processed
    # Note that this may not work depending on the active branch of pcl_processing_ros2.
    # It works on branch volume_recalculation, but this may not be the latest method for calculating the volumes.

    REPROCESS_PCLS = False 
    OVERWRITE_AREA = None # IN mm2

    # Path to the file to be processed
    data_path = Path('/workspaces/BrightSkyRepoLinux/') 

    # An identifier for the files that have to be processed 
    test_identifiers = ['']

    bags = [path for path in data_path.iterdir() if 'rosbag' in str(path) and any([identifier in str(path) for identifier in test_identifiers])]

    if OVERWRITE_AREA is not None:
        print(f'[WARNING]: OVERWRITE_AREA is set to {OVERWRITE_AREA}. Make sure this is intentional. If not, set it to None')

    
    if REPROCESS_PCLS:
        recalc_volumes = recalculate_volumes(bags)
        
    if not REPROCESS_PCLS:
        for bag in bags:
            try:
                convert_bag(bag, overwrite_area=OVERWRITE_AREA)
            except Exception as e:
                print(f'Error: {e}')
                print(f'Skipping bag {bag}')
                continue 
    else:
        for bag, volume in zip(bags, recalc_volumes):
            try:
                from pcl_processing_ros2 import PCLfunctions # TODO moved this here because this is not acutally in the file on the main branch right now
                convert_bag(bag, volume)
            except Exception as e:
                print(f'Error: {e}')
                print(f'Skipping bag {bag}')
                continue 