import os
import pandas as pd

path_to_hdf = "/mnt/usfboxsync/Boris Blanck Lab Work/Kidney2/vdj_recoveries_cptac.h5"
output_path = "/mnt/usfboxsync/Boris Blanck Lab Work/Kidney2/vdj_cptac/"

with pd.HDFStore(path_to_hdf) as hdf:
    keys = hdf.keys()
    
for key in keys:
    print("Converting %s"%key)
    df = pd.read_hdf(path_to_hdf, key)
    df.to_csv(output_path+key+".csv", index=False)