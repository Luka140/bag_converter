import numpy as np 
import matplotlib.pyplot as plt 
import pandas as pd 
import pathlib



data = pd.read_csv('data_compilation/data_gathering.csv')


print(data[['grind_time', 'avg_force']])

# time_normalized_removal = data['removed_material'] / data['grind_time']
# print(time_normalized_removal)

# mask = abs(data['grind_time_setpoint'] - 30.0) <1e-3 

# plt.scatter(data['initial_wear'], data['removed_material'] / data['grind_time'])
# plt.xlabel('Belt wear: sum(force $\cdot$ rpm $\cdot$ grind time)')
# plt.ylabel('Removed material [mm3/s]')
# plt.show()