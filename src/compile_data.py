import pandas as pd
import os

# ================================ Inputs ================================================

filename = 'rosbag2_2024-08-16_13:56:28_sample0.1__f3_rpm8000_grit60_t10.0_th0.0_d5.0.csv'
data_collection_path = 'data_compilation/datapoints.csv'

# ========================================================================================

# Infer test settings from filename 
parameter_list          = filename.split('__')[1].strip('.csv').split('_')
force_setpoint          = float(parameter_list[0].strip('f'))
rpm_setpoint            = float(parameter_list[1].strip('rpm'))
grit                    = float(parameter_list[2].strip('grit'))
grind_time_setpoint     = float(parameter_list[3].strip('t'))
thickness_plate         = float(parameter_list[4].strip('th'))
removed_material_depth  = float(parameter_list[5].strip('d'))

assert ('f'     in parameter_list[0] and
        'rpm'   in parameter_list[1] and
        'grit'  in parameter_list[2] and
        't'     in parameter_list[3] and
        'th'    in parameter_list[4] and
        'd'     in parameter_list[5]), 'The order or labelling of the data in the filename seems to have changed - adjust the script accordingly'

try:
    current_database = pd.read_csv(data_collection_path, sep=',')
    if current_database.source.eq(filename).any():
        raise UserWarning(f'The rosbag file specified has already been entered into the database.\n' 
                        f'Consider removing one of the entries for the following file: {filename}')
except pd.errors.EmptyDataError:
    ...
    
bag_csv_folder_path = 'csv_bags'
data = pd.read_csv(f'{bag_csv_folder_path}/{filename}', sep=',')    

force_close_to_target = abs((data['force'] - data['force_cmd']) / (1e-6 + data['force'])) < 0.1 
active_data_points = data[force_close_to_target]
active_data_points = active_data_points[active_data_points['force'] > 0]

grind_time_s = (active_data_points['timestamp'].max() - active_data_points['timestamp'].min()) / 10**9
avg_rpm = active_data_points['rpm'].mean()
avg_force = active_data_points['force'].mean()


entries = [grind_time_s, avg_rpm, avg_force, grit, thickness_plate, removed_material_depth, force_setpoint, rpm_setpoint, grind_time_setpoint]
headers = ['grind_time', 'avg_rpm', 'avg_force', 'grit', 'thickness_plate', 'removed_material', 'force_setpoint', 'rpm_setpoint', 'grind_time_setpoint','source']

if not os.path.isfile(data_collection_path):
    with open(data_collection_path, 'w'):
        print("File created")

with open(data_collection_path, 'r+') as f:
    current_data = [line for line in f.readlines()]
    if len(current_data) < 1:
        f.write(f"{','.join(headers)}\n")
    f.write(f"{','.join([str(entry) for entry in entries])},{filename}\n")

