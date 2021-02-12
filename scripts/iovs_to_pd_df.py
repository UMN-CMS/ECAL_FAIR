import pandas as pd
import h5py
import numpy as np

def dfSlicesFromDS(xtals_ds, xtals_idxs=None):
    #now we grab a slice (default [:,:]) and convert that np array to a pandas dataframe
    if(xtals_idxs == None):
        np_slice = xtals_ds[:,:]
    else:
        np_slice = xtals_ds[:,xtals_idxs]
    slice_df = pd.DataFrame(data=np_slice)

    return pd.DataFrame(data=np_slice) 
   

def pullIOVtoDF(filename, corrections, xtals_idxs=None):
    #read in iov file
    hd_file = h5py.File('../ECAL_RADDAM_Data/2018/iov.w447.hdf5', 'r')
    
    #get crystals group
    
    xtals_grp = hd_file['crystals']
    
   # correction_dfs = {}
    correction_dfs = []
    #get correction dataframes
    for correction in corrections:
         #correction_dfs[correction] = dfSlicesFromDS(xtals_grp[correction], xtals_idxs)
         correction_dfs.append(dfSlicesFromDS(xtals_grp[correction], xtals_idxs))
    corr_df = pd.concat(correction_dfs, axis=1)
    #we want to rename the columns
    corr_df.columns = corrections
    return corr_df
