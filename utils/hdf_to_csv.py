import os
import pandas as pd

#set path to hdf file
path_to_hdf = "/usfboxsync/vdj_recoveries_cptac.h5"

#set output path
output_path = "/usfboxsync/vdj_cptac/"

#pull all keys in hdfstore
with pd.HDFStore(path_to_hdf) as hdf:
    keys = hdf.keys()

#loop for reading df from each key and outputting it to csv file
for key in keys:
    print("Converting %s"%key)
    df = pd.read_hdf(path_to_hdf, key)
    df.to_csv(output_path+key+".csv", index=False)