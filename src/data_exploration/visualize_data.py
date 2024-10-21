import numpy as np 
import matplotlib.pyplot as plt 
import pandas as pd 
import matplotlib.colors as mcolors
import copy


def load_filter_data(path):
    grind_data = pd.read_csv(path)

    # Delete rows where removed_material is less than 12
    grind_data = grind_data[grind_data['removed_material'] >= 5]

    # Filter out points which have mad of more than 1000
    grind_data = grind_data[grind_data['mad_rpm'] <= 1000]

    # Filter out avg rpm that is lower than half of rpm_setpoint
    grind_data = grind_data[grind_data['avg_rpm'] >= grind_data['rpm_setpoint'] / 2]

    data = grind_data[pd.isna(grind_data['failure_msg'])]
    return data

def plot_time_removal(filtered_data: pd.DataFrame):
    data = copy.deepcopy(filtered_data)

    # Group the DataFrame by 'force_setpoint' and 'rpm_setpoint'
    grouped_df = data.groupby(['force_setpoint', 'rpm_setpoint'])

    # Iterate over the groups and process them
    colors = list(mcolors.TABLEAU_COLORS) + list(mcolors.BASE_COLORS)
    for i, ((force, rpm), group) in enumerate(grouped_df):
        if group.shape[0] < 3:
            continue 
        print(f"Force Setpoint: {force}, RPM Setpoint: {rpm}")
        print(group)
        print("\n")
        
        plt.scatter(group['grind_time'], group['removed_material'], color=colors[i])
        sorted_group = group.sort_values('removed_material')
        plt.plot(sorted_group['grind_time'].to_numpy(), sorted_group['removed_material'].to_numpy(), color=colors[i], label=f'F {force} - RPM {rpm}')

    plt.xlabel('Grind time [s]')
    plt.ylabel('Removed material [mm3]')
    plt.legend()
    plt.title("Material removal as a function of grind time")

    plt.show()

def plot_full_wear_data(data: pd.DataFrame):
        
    plt.scatter(data['initial_wear'], data['removed_material'] / data['grind_time'])
    plt.xlabel('Belt wear: sum(force $\cdot$ rpm $\cdot$ grind time)')
    plt.ylabel('Material removal rate [mm3/s]')
    plt.show()

def plot_wear_relation(data: pd.DataFrame):
    wear_data = copy.deepcopy(data)

    grouped_df = wear_data.groupby(['force_setpoint', 'rpm_setpoint', 'grind_time_setpoint'])

    colors = list(mcolors.TABLEAU_COLORS) + list(mcolors.BASE_COLORS)
    for i, ((force, rpm, time), group) in enumerate(grouped_df):
        if group.shape[0] < 2:
            continue

        print(f"Force Setpoint: {force}, RPM Setpoint: {rpm}, Time Setpoint {time}")
        print(group)
        print("\n")

        plt.scatter(group['initial_wear'], group['removed_material'] / group['grind_time'], color=colors[i], label=f'F: {force} - RPM: {rpm} - T: {time}')
        plt.xlabel('Belt wear: sum(force $\cdot$ rpm $\cdot$ grind time)')
        plt.ylabel('Material removal rate [mm3/s]')
    
    plt.legend()
    plt.show()


if __name__ == '__main__':

    data = load_filter_data('data_compilation/datapoints_post_volume_fix.csv')
    # plot_time_removal(data)

    wear_data = pd.read_csv('data_compilation/datapoints_weartestV3.csv')
    # plot_full_wear_data(wear_data)

    plot_wear_relation(wear_data)