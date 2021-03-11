from elmonk.common.setuptools import data_path, download, elmonk_basepath
import pandas as pd
import os

#os.chdir(elmonk_basepath)

from elmonk.common import HdfLaser

def pullSlimAndSkim(dst_file, oms_file, out_file=None, slim=True):
    #This is an HdfLaser object, it also loads lumi info from the oms file
    data = HdfLaser(dst_file, hdf_run_info=oms_file)
    data.add_inst_lumi_info()
    
    #This contains a dataframe which indexes the crystals across lots of different ways to slice the detector we use it to choose specific crystals
    from ecalic import geom
    
    #this grabs the text string and uses it as a pandas dataframe query since geom is literally an emap as a pandas.dataframe. It has many columns, including FED... and eta.
    print('Grabbing Crystal Indices')
    xtals = data.xtal_idx('(eta_module == 4) & (phi_module == 1)')
    #This is a pandas dataframe with two columns (iov_idx and POSIX paths to iovs) in my case, they are all just paths to the same file. If your data was across multiple files, I guess this would tell the program where to find them.
    print('Grabbing IOV histories')
    iovs = data.iov_idx() #this just gets all of them
    #This gets the calibration values
    histories = data.xtal_history(iov_idx=iovs,xtal_idx=xtals)
    
    #grab the first crystal
    crystal_history = histories.iloc[:,0]
    #grab the iov idx (this is a bit horrible, but I'm not sure of a better way at the moment)
    iovdx = iovs.loc[:, 'iov_idx'].reset_index().loc[:, 'iov_idx']
    
    #make new dataframe
    iov_df = pd.DataFrame(columns=['calibration','iov_idx'])
    
    #set calibration column
    iov_df['calibration'] = crystal_history
    
    #push time idx to new column
    iov_df = iov_df.reset_index()
    
    #add iov idx column
    iov_df['iov_idx'] = iovdx
    
    #merge iov_idx into lumi df
    lumi_df = pd.merge(iovdx, data.info)
    
    #merge lumi_df with iov_df
    full_df = pd.merge(lumi_df, iov_df, on='iov_idx')
    
    #just grab some of the interesting columns
    wanted_columns = ['iov_idx','date_x','calibration','inst_lumi']
    if(slim == True): 
        print('only taking colums: ')
        print(wanted_columns)
        slim_df = full_df.loc[:, wanted_columns]
    
    else:
        slim_df = full_df
    #quick rename
    slim_df = slim_df.rename(columns={'date_x':'datetime'})

    #make cumulative inst_lumi column
    slim_df['int_inst_lumi'] = slim_df.inst_lumi.fillna(0.0).cumsum()
    
    #write it out or return the dataframe
    if not (out_file==None):
        slim_df.to_csv(out_file,index=True)
        return
    else:
        return slim_df, xtals
