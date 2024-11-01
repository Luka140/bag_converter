import pandas as pd
import os
import pathlib

"""
This script reads the sequential time .csv file from a test and processes it to extract a single datapoint.
The following test settings are inferred from the filename, which should be generated by the data_gathering package

    - force_setpoint        
    - rpm_setpoint          
    - grit                  
    - grind_time_setpoint   
    - thickness_plate       
    - removed_material_depth

The following data is extracted from the rosbag csv:
    - grind_time
    - avg_rpm
    - avg_force

"""


def compile_data_from_csv_bag(filename, bag_csv_folder_path, compilation_path, contact_force_threshold):
    # Infer test settings from filename 
    parameter_list          = filename.split('__')[2].strip('.csv').split('_')
    force_setpoint          = float(parameter_list[0].strip('f'))
    rpm_setpoint            = float(parameter_list[1].strip('rpm'))
    grit                    = float(parameter_list[2].strip('grit'))
    grind_time_setpoint     = float(parameter_list[3].strip('t'))
    # thickness_plate         = float(parameter_list[4].strip('th'))
    grinded_volume          = float(parameter_list[4].strip('v'))
    wear                    = float(parameter_list[5].strip('w'))
    grind_area              = float(parameter_list[6].strip('a'))

    assert ('f'     in parameter_list[0] and
            'rpm'   in parameter_list[1] and
            'grit'  in parameter_list[2] and
            't'     in parameter_list[3] and
            # 'th'    in parameter_list[4] and
            'v'     in parameter_list[4] and 
            'w'     in parameter_list[5] and
            'a'     in parameter_list[6]
            ), 'The order or labelling of the data in the filename seems to have changed - adjust the script accordingly'


    if not os.path.isfile(compilation_path):
        with open(compilation_path, 'w'):
            print("File created")

    try:
        current_database = pd.read_csv(compilation_path, sep=',')
        if current_database.source.eq(filename).any():
            raise UserWarning(f'The rosbag file specified has already been entered into the database.\n' 
                            f'Consider removing one of the entries for the following file: {filename}')
    except pd.errors.EmptyDataError:
        ...
        
    data = pd.read_csv(f'{bag_csv_folder_path}/{filename}', sep=',')    

    # Find the initial grinding start time by detecting when the force reaches within the threshold
    force_close_to_target = abs((data['force'] - data['force_cmd']) / (1e-6 + data['force'])) < contact_force_threshold 
    active_data_points = data[force_close_to_target]
    active_data_points = active_data_points[active_data_points['force'] > 0]

    grind_time_s = (active_data_points['timestamp'].max() - active_data_points['timestamp'].min()) / 10**9
    avg_rpm = active_data_points['rpm'].mean()
    mean_absolute_deviation_rpm = abs(active_data_points['rpm'] - avg_rpm).mean()
    avg_force = active_data_points['force'].mean()
    mean_absolute_deviation_force = abs(active_data_points['force'] - avg_force).mean()
    avg_position = active_data_points['position'].mean()
    mean_absolute_deviation_position = abs(active_data_points['position'] - avg_position).mean()
    
    failure_msg = active_data_points['failure_msg'].iloc[0]


    entries = [grind_time_s, avg_rpm, mean_absolute_deviation_rpm, avg_force, mean_absolute_deviation_force, avg_position, mean_absolute_deviation_position, grit, grinded_volume, grind_area, wear, force_setpoint, rpm_setpoint, grind_time_setpoint, failure_msg]
    headers = ['grind_time', 'avg_rpm', 'mad_rpm', 'avg_force', 'mad_force', 'avg_position', 'mad_position', 'grit', 'removed_material', 'grind_area', 'initial_wear', 'force_setpoint', 'rpm_setpoint', 'grind_time_setpoint', 'failure_msg']

    with open(compilation_path, 'r+') as f:
        current_data = [line for line in f.readlines()]
        if len(current_data) < 1:
            f.write(f"{','.join(headers)},source\n")
        f.write(f"{','.join([str(entry) for entry in entries])},{filename}\n")


if __name__ == '__main__':


    # The .csv which containts the 'database' to add to, on which a model will be trained
    compiled_data_file_name = 'quadruple_plate.csv'
    data_collection_path = pathlib.Path('data_compilation') / compiled_data_file_name
    csv_folder_path = pathlib.Path('csv_bags')
    test_identifiers = ['quadruple_plate_reset_new_belt']

    # The .csv rosbag from which data should be extracted 
    # filename = 'rosbag2_2024-08-16_13:56:28_sample0.1__f3_rpm8000_grit60_t10.0_th0.0_d5.0.csv'
    filenames = [path.name for path in csv_folder_path.iterdir() if any([identifier in path.name for identifier in test_identifiers])]

    # This setting dictates force what force the grind time should start counting
    # A value of 0.1 means that the 'grinding time' starts from the moment that the force initially reaches within 10% of the target force 
    force_threshold = 0.1

    # if not data_collection_path.is_file:
    #     data_collection_path.touch()

    for filename in filenames:
        try:
            compile_data_from_csv_bag(filename, str(csv_folder_path), data_collection_path, force_threshold)
        except UserWarning as e:
            pass 