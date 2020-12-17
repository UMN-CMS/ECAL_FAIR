#THIS CODE OPENS UP ECAL LASER HDF5 FILES AND MAKES SOME PLOTS.
#THE MAIN GOAL IS REPRODUCING COMPONENTS OF THE "MONEY PLOT" FOR VERIFICATION
from pandas import (
    DataFrame, HDFStore
)
import pandas as pd
import numpy as np

hd_file = HDFStore('../ECAL_RADDAM_Data/2016/dst.w447.hdf5')
print(hd_file)
print(hd_file.keys())
for key in hd_file.keys():
    print(hd_file[key])
hd_file.close()
