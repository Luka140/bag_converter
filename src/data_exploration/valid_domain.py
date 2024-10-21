import numpy as np
import matplotlib.pyplot as plt
import pathlib
import pandas as pd
from concave_hull import concave_hull, concave_hull_indexes
from scipy.spatial import ConvexHull

# Load data
data_folder = pathlib.Path.cwd() / 'data_compilation'

csv_files = [
    'data_gathering.csv',
    'datapoints_post_volume_fix.csv'
]

csvs = []
for filename in csv_files:
    csvs.append(pd.read_csv(data_folder / filename, delimiter=','))

data = pd.concat(csvs, ignore_index=True, axis=0)

removal_threshold = 3.

# DataFrame of successful tests
df_no_failure = data[data['failure_msg'].isna()]

# DataFrame where 'failure_msg' contains 'belt RPM'
df_belt_rpm_failure = data[data['failure_msg'].str.contains('belt RPM', na=False)]

# DataFrame where 'failure_msg' does not contain 'belt RPM'
df_no_belt_rpm_failure = data[~data['failure_msg'].str.contains('belt RPM', na=False)]

# DataFrame for removed material less than threshold
df_low_removed_material = df_no_failure[df_no_failure['removed_material'] < removal_threshold]

rpm_working_array = df_no_belt_rpm_failure[['force_setpoint', 'rpm_setpoint']].to_numpy()
rpm_failure_array = df_belt_rpm_failure[['force_setpoint', 'rpm_setpoint']].to_numpy()

unique_rpms = sorted(set([*rpm_failure_array[:, 1].flatten(), *rpm_working_array[:, 1].flatten()]))

# Convert rpm_failure_array to a list to allow modifications
rpm_failure_list = rpm_failure_array.tolist()

# Propagate failures 
for rpm in unique_rpms:
    # Get all failures for the current RPM in the failure array
    rpm_in_failure = rpm_failure_array[rpm_failure_array[:, 1] == rpm]
    
    if rpm_in_failure.size == 0:
        # If no failures for this rpm, skip to the next
        continue
    
    # Get all forces where this rpm failed
    failure_forces = rpm_in_failure[:, 0]

    for failure_force in failure_forces:
        # 1. Propagate the failure downward to all lower RPMs for the same failure_force
        lower_rpms = [r for r in unique_rpms if r < rpm]
        for lower_rpm in lower_rpms:
            rpm_failure_list.append([failure_force, lower_rpm])
        
        # 2. Propagate the failure upward to all higher forces for the same RPM
        higher_forces = df_no_belt_rpm_failure[df_no_belt_rpm_failure['force_setpoint'] > failure_force]['force_setpoint'].unique()
        for higher_force in higher_forces:
            rpm_failure_list.append([higher_force, rpm])

# Convert the rpm_failure_list back to a numpy array
rpm_failure_array = np.array(rpm_failure_list)

# Remove duplicates from the updated failure array
rpm_failure_array = np.unique(rpm_failure_array, axis=0)

# Concave hull algorithm to get the boundary
idxes = concave_hull_indexes(
    rpm_failure_array,
    length_threshold=0.1,
)
boundary = rpm_failure_array[idxes]
hull_2d = ConvexHull(boundary)

boundary_line = np.vstack((hull_2d.points[hull_2d.vertices], hull_2d.points[hull_2d.vertices][0,:]))

# Compute the convex hull for points where removed_material < 3
low_removed_material_array = df_low_removed_material[['force_setpoint', 'rpm_setpoint']].to_numpy()

if len(low_removed_material_array) > 2:
    convex_hull_removed_material = ConvexHull(low_removed_material_array)
    hull_removed_material_boundary = np.vstack((
        low_removed_material_array[convex_hull_removed_material.vertices],
        low_removed_material_array[convex_hull_removed_material.vertices][0, :]
    ))

# Plotting
plt.figure(figsize=(10, 6))

# Plot the 'RPM Failure' region
plt.fill(boundary_line[:, 0], boundary_line[:, 1], color='red', alpha=0.3, label='RPM Failure Zone')

# Plot the 'Low Removed Material' region
if len(low_removed_material_array) > 2:
    plt.fill(hull_removed_material_boundary[:, 0], hull_removed_material_boundary[:, 1], color='yellow', alpha=0.3, label='Low Material Removal Zone')

# Scatter plots for actual points
plt.scatter(df_no_belt_rpm_failure['force_setpoint'], df_no_belt_rpm_failure['rpm_setpoint'], color='blue', s=30, alpha=0.7, label='Successfull tests', edgecolor='black')
plt.scatter(df_belt_rpm_failure['force_setpoint'], df_belt_rpm_failure['rpm_setpoint'], color='red', s=50, alpha=0.7, label='RPM Failure', edgecolor='black')

# Labels and title
plt.xlabel('Force Setpoint [N]', fontsize=12)
plt.ylabel('RPM Setpoint', fontsize=12)
plt.title('Operational Zones with Low Material Removal and RPM Failure', fontsize=14)

# Grid, legend, and ticks
plt.grid(True, linestyle='--', alpha=0.5)
plt.legend(fontsize=10)
plt.tight_layout()

# Show the plot
plt.show()
