
# coding: utf-8

# In[1]:

import os
import sys
project_dir = os.path.join(os.pardir, os.pardir)
sys.path.append(project_dir)

import pandas as pd
import numpy as np
import pulp
import time
from IPython.display import display


#  Number of Equipments and Census Zones:

# In[4]:

df_sectors = pd.read_csv(project_dir + "/data/external/od/area_setoresCensitarios.csv", sep=";")
sector_list = df_sectors["cd_geocodi"].unique().tolist()
max_setores = len(sector_list)
print("Initial number of sectors: ",max_setores)
display(df_sectors.head())

df_equip = pd.read_csv(project_dir + "/data/interim/od/avg_per_quarter_815.csv")
df_equip.drop(df_equip.columns[0], axis=1, inplace=True)
equip_list = df_equip["Equipamento"].unique().tolist()
max_radares = len(equip_list)
print("Initial number of radars: ", max_radares)
display(df_equip.head())


# Note that several equipment have flow in two directions:

# In[5]:

direction_per_equip = df_equip.groupby("Equipamento").agg({"Sentido": "count"})
direction_per_equip.sort_values("Sentido", ascending=False).head()


# We will use the sum of the flow in both directions as weight for the allocation of Census Zones per equipment.

# In[6]:

total_per_equip = df_equip.groupby("Equipamento").agg({"Total": "sum"})
total_per_equip["proporcao_fluxo"] = total_per_equip["Total"] / total_per_equip["Total"].sum()
total_per_equip["num_sectors"] = total_per_equip["proporcao_fluxo"]*max_setores
total_per_equip["num_sectors"] = total_per_equip["num_sectors"].round()
diff = round(total_per_equip["num_sectors"].sum() - max_setores)
total_per_equip.iloc[0:abs(diff), total_per_equip.columns.get_loc("num_sectors")] -= np.sign(diff)
total_per_equip["num_sectors"] = total_per_equip["num_sectors"].astype(int)
print("Number of sectors after allocation: ", total_per_equip["num_sectors"].sum())
print("Checking number of equipment: ", len(total_per_equip))
total_per_equip.head()


# Distance matrix:

# In[8]:

df_distance_matrix = pd.read_csv(project_dir + "/data/external/od/Matriz_distancias.csv", index_col=0)
df_distance_matrix.head()


# Optimization code:

# In[9]:

start = time.time()
prob = pulp.LpProblem("Designation Problem", pulp.LpMinimize)
possibleChoices = [(s,r) for s in sector_list for r in equip_list]

choice_equip_sector = pulp.LpVariable.dicts("choice",(sector_list,equip_list),0,1,pulp.LpInteger)

#objective function
objective_function = [choice_equip_sector[sector][equip]*df_distance_matrix.loc[sector,equip]
                      for (sector,equip) in possibleChoices]
prob += pulp.lpSum(objective_function)

#each sector must have at most 1 equipment
for sector in sector_list:
    vars_to_sum = [choice_equip_sector[sector][equip] for equip in equip_list]
    prob += pulp.lpSum(vars_to_sum) == 1

#each equipment must have at most "num_sectors" sectors
for equip in equip_list:
    vars_to_sum = [choice_equip_sector[sector][equip] for sector in sector_list]
    prob += pulp.lpSum(vars_to_sum) <= total_per_equip.loc[equip, "num_sectors"]
    
prob.solve()
end = time.time()
elapsed_time = str(int(end-start))
print("Optimization run in " + elapsed_time + " seconds.")

solutions_list = []
for sector in sector_list:
    for equip in equip_list:
        if choice_equip_sector[sector][equip].value() == 1.0:
            solutions_list.append((sector, equip))


# In[11]:

df_solutions = pd.DataFrame(solutions_list, columns=["Sector", "Equipment"])
print("Number of sectors after optimization: ", df_solutions["Sector"].nunique())
print("Number of radars after optimization: ", df_solutions["Equipment"].nunique())

#Cross-check on number of sectors per equipment
sectors_per_equip = df_solutions.groupby("Equipment").agg({"Sector": "count"})
sectors_per_equip = sectors_per_equip.join(total_per_equip["num_sectors"])
sectors_per_equip["diff"] = abs(sectors_per_equip["Sector"] - sectors_per_equip["num_sectors"])
total_diff = sectors_per_equip["diff"].sum()
print("Sum of the differences between actual and maximum number of sectors per radar: ", total_diff)

df_solutions.to_csv(project_dir + "/data/processed/od/optimized_sectors_per_radar.csv", index=False)

