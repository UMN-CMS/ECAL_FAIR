from elmonk.common.setuptools import data_path, download, elmonk_basepath
import pandas as pd

dstPath = data_path / 'dst.merged/2018'
if not dstPath.exists():
    download('dst_fed636_2018.tuto.tar.gz')
if not (data_path / 'oms_db.light.2018.hdf5').exists():
    download('oms_db.light.2018.hdf5')
import os

os.chdir(elmonk_basepath)

from elmonk.common import HdfLaser
#This is an HdfLaser object
#If you ask for .info, you get a pandas dataframe
#It appears to be of shape (8185, 10)
#It has the following columns:
#'run', 'seq', 'fill', 'bfield', 'temperature', 'good', 't1', 'date', 'iov_idx', 'hdf_name'
data = HdfLaser('/panfs/roc/groups/4/rusack/evans908/FAIR/ECAL_RADDAM_Data/2018/dst.w447.hdf5', hdf_run_info='/panfs/roc/groups/4/rusack/evans908/FAIR/ECAL_RADDAM_Data/2018/oms.hdf5')
data.add_inst_lumi_info()

print(data.info.columns)

from ecalic import geom

#This returns an index (int64) with 1700 entries (I'm guessing these are all of the crystals that are part of FED 636
#this grabs the text string and uses it as a pandas dataframe query since geom is literally an emap as a pandas.dataframe. It has many columns, including FED... and eta.
print('Grabbing Crystal Indices')
xtals = data.xtal_idx('(eta < 0.1) & (eta > -0.1)')
#This is a pandas dataframe with two columns (iov_idx and POSIX paths to iovs) in my case, they are all just paths to the same file. If your data was across multiple files, I guess this would tell the program where to find them.
#iovs = data.iov_idx(['2016-08-01','2016-08-10'])
#This is also a pandas dataframe, it has a column for each crystal in the FED (1700) and then it has rows for each time there was laser data taken in the data range I specified. It looks like there are 344 rows.
#new date range
print('Grabbing IOV histories')
iovs = data.iov_idx() #this is the full 2018 date range
histories = data.xtal_history(iov_idx=iovs,xtal_idx=xtals)

import numpy as np

xtals_to_plot = np.random.choice(histories.columns,5)
#xtals_to_plot = histories

import ecalic.cmsStyle
import matplotlib.pyplot as plt

ax = histories.plot(y=xtals_to_plot,linestyle='none',marker='.', figsize=[20,10])
ax.set_ylim(0.80,1.05)
ax.set_title('Histories')
ax.legend(ncol=5)
print(ax)

#lumi = data['r_lumi'].astype(float)
#lumi.fillna(0)
#lumi.hist(bins=10)

import elmonk.common.stats as stats
central = data.xtal_history(iov_idx=iovs, xtal_idx=xtals, func=stats.history_quantiles)

import elmonk.common.plotting as plotting
plotting.brazilian_flag(pd.DataFrame(central),{0:'The histories of the most central crystals'})

plt.savefig("lumi.png")
