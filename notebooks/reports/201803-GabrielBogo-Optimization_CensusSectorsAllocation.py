
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
from shapely.geometry import Polygon


# In[2]:

#Data reading and cleaning
df_sectors = pd.read_csv(project_dir + "/data/external/od/setores_censitarios_CSV.csv",
                         encoding="latin-1")
df_sectors.WKT = df_sectors.WKT.str.replace("POLYGON", "")
df_sectors.WKT = df_sectors.WKT.str.replace("\(", "")
df_sectors.WKT = df_sectors.WKT.str.replace("\)", "")
df_sectors.WKT = df_sectors.WKT.str.strip()

#Extract coordinates from string
df_sectors["coords"] = df_sectors.WKT.str.split(",")
df_sectors.coords = df_sectors.coords.apply(lambda x: [tuple(map(float, i.split(" "))) for i in x])

#Create shapely Polygon
df_sectors["polygon"] = df_sectors.coords.apply(lambda x: Polygon(x))

#Get polygon area
df_sectors["area"] = df_sectors.polygon.apply(lambda x: x.area)

#Check data
print(len(df_sectors), "rows")
print(df_sectors.objectid.nunique(), "objectids")

#Drop unused columns and index with cd_geocodi
df_sectors = df_sectors[["cd_geocodi", "tipo", "nm_bairro", "polygon", "area"]]
df_sectors.set_index("cd_geocodi", inplace=True, verify_integrity=True)

df_sectors.head()


#  Number of Equipments and Census Zones:

# In[3]:

sector_list = df_sectors.index.unique().tolist()
max_setores = len(sector_list)
print("Initial number of sectors: ",max_setores)

df_equip = pd.read_csv(project_dir + "/data/interim/od/avg_per_quarter_815.csv")
df_equip.drop(df_equip.columns[0], axis=1, inplace=True)
equip_list = df_equip["Equipamento"].unique().tolist()
max_radares = len(equip_list)
print("Initial number of radars: ", max_radares)
display(df_equip.head())


# Note that several equipment have flow in two directions:

# In[4]:

direction_per_equip = df_equip.groupby("Equipamento").agg({"Sentido": "count"})
direction_per_equip.sort_values("Sentido", ascending=False).head()


# We will use the sum of the flow in both directions as weight for the allocation of Census Zones per equipment.

# In[5]:

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


# Get Distance Matrix exported from QGis:

# In[6]:

df_distance_matrix = pd.read_csv(project_dir + "/data/external/od/Matriz_distancias.csv", index_col=0)
df_distance_matrix.head()


# Build Adjacency Matrix:

# In[29]:

start = time.time()
adjacency_matrix = pd.DataFrame(0, index=df_sectors.index, columns=df_sectors.index)
def build_adjacency_matrix():
    for index1,_ in df_sectors.iterrows():
        for index2,_ in df_sectors.iterrows():
            if index1 == index2:
                intersects = 1
            else:
                try:
                    intersects = int(df_sectors.loc[index1, "polygon"].intersects(df_sectors.loc[index2, "polygon"]))
                except:
                    intersects = 1 #errors are probably being caused by wrong Linestrings in adjacent Polygons                                        

            adjacency_matrix.loc[index1, index2] = intersects
            
            return adjacency_matrix
        
#adjacency_matrix = build_adjacency_matrix()
adjacency_matrix = pd.read_excel(project_dir + "/data/interim/od/adjacency_matrix.xlsx", index_col=0)
end = time.time()
elapsed_time = str(int(end-start))
print("Matrix created in " + elapsed_time + " seconds.")

adjacency_matrix.head()


# In[8]:

df_sectors.loc[420910205000341, "polygon"]


# In[9]:

df_sectors.loc[420910205000737, "polygon"]


# In[10]:

df_sectors.loc[420910205000458, "polygon"]


# In[11]:

df_sectors.loc[420910205000459, "polygon"]


# In[12]:

df_sectors.loc[420910205000608, "polygon"]


# Optimization code:

# In[25]:

start = time.time()
prob = pulp.LpProblem("Designation Problem", pulp.LpMinimize)
possibleChoices = [(sector,equip) for sector in sector_list for equip in equip_list]

choice_equip_sector = pulp.LpVariable.dicts("choice",(sector_list,equip_list),0,1,pulp.LpInteger)

#objective function
objective_function = [choice_equip_sector[sector][equip]*df_distance_matrix.loc[sector,equip]
                      for (sector,equip) in possibleChoices]
prob += pulp.lpSum(objective_function)

#each sector must have at most 1 equipment
for sector in sector_list:
    vars_to_sum = [choice_equip_sector[sector][equip] for equip in equip_list]
    prob += pulp.lpSum(vars_to_sum) == 1
    
#each equipment must have at least 1 sector
for equip in equip_list:
    vars_to_sum = [choice_equip_sector[sector][equip] for sector in sector_list]
    prob += pulp.lpSum(vars_to_sum) >= 1

# #each equipment must have at most "num_sectors" sectors
# for equip in equip_list:
#     vars_to_sum = [choice_equip_sector[sector][equip] for sector in sector_list]
#     prob += pulp.lpSum(vars_to_sum) <= total_per_equip.loc[equip, "num_sectors"]
#     #prob += pulp.lpSum(vars_to_sum) <= 8

#only contiguous sectors are allowed

for j in range(0, len(sector_list)):
    for k in range(j, len(sector_list)):
        for i in equip_list:
            sector1 = sector_list[j]
            sector2 = sector_list[k]
            vars_to_sum = []
            vars_to_sum.append(choice_equip_sector[sector1][i])
            vars_to_sum.append(choice_equip_sector[sector2][i])
        prob += pulp.lpSum(vars_to_sum) <= (1 + adjacency_matrix.loc[sector1,sector2])

end = time.time()
elapsed_time = str(int(end-start))
print("Começando a resolução...", elapsed_time)
prob.solve(pulp.PULP_CBC_CMD(fracGap=1, msg=True))
print("Status: ", pulp.LpStatus[prob.status])
end = time.time()
elapsed_time = str(int(end-start))
print("Optimization run in " + elapsed_time + " seconds.")

solutions_list = []
for sector in sector_list:
    for equip in equip_list:
        if choice_equip_sector[sector][equip].value() == 1.0:
            solutions_list.append((sector, equip))


# In[25]:

df_solutions = pd.DataFrame(solutions_list, columns=["Sector", "Equipment"])
print("Number of sectors after optimization: ", df_solutions["Sector"].nunique())
print("Number of radars after optimization: ", df_solutions["Equipment"].nunique())

#Cross-check on number of sectors per equipment
sectors_per_equip = df_solutions.groupby("Equipment").agg({"Sector": "count"})
sectors_per_equip = sectors_per_equip.join(total_per_equip["num_sectors"])
sectors_per_equip["diff"] = abs(sectors_per_equip["Sector"] - sectors_per_equip["num_sectors"])
total_diff = sectors_per_equip["diff"].sum()
print("Sum of the differences between actual and maximum number of sectors per radar: ", total_diff)

df_solutions.to_csv(project_dir + "/data/processed/od/optimized_sectors_per_radar_nomax_1min.csv", index=False)

