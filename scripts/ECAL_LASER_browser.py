#THIS CODE OPENS UP ECAL LASER HDF5 FILES AND MAKES SOME PLOTS.
#THE MAIN GOAL IS REPRODUCING COMPONENTS OF THE "MONEY PLOT" FOR VERIFICATION
from pandas import (
    DataFrame, HDFStore
)
import h5py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

hd_file = h5py.File('../ECAL_RADDAM_Data/2018/iov.w447.hdf5', 'r')
key_list = list(hd_file.keys())
print("##################TOP LEVEL################")
print(key_list)
for key in key_list:
   print("    #######################GROUPS#######################")
   dset = hd_file[key]
   print("    ", end='')
   print(dset.keys())
   for subset in dset.keys():
       print("        #####################SUBGROUPS#####################")
       print("        ", end='')
       print(subset)
       for idx, subsub in enumerate(dset[subset]):
           try:
               theType = subsub.dtype
           except AttributeError:
               theType = type(subsub) 
           try:
               theShape = subsub.shape
           except AttributeError:
               theShape = "NA"
           print("            #######################INDIVIDUALS##############")
           print("            #### IDX: {}".format(idx))
           print("            #### dtype: {} shape: {}".format(theType, theShape))
           print("            ", end='')
           print(subsub)
           if idx == 3:
               break
hd_file.close()
