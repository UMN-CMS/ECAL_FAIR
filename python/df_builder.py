import pandas as pd
import h5py
import numpy as np
import os
from elmonk.common.setuptools import data_path, download, elmonk_basepath
from elmonk.common import HdfLaser


def dfSlicesFromDS(xtals_ds, xtals_idxs=None):
    '''
    A function to extract crystal slices from an HDF5 dataset

    This function takes two arguments:
        xtals_ds -- an hdf5 dataset
        xtals_idxs -- a list of crystal indices (defaulting to all crystals in the dataset) 
        
    It returns a pandas dataframe of the specified slice
    '''
    #now we grab a slice (default [:,:]) and convert that np array to a pandas dataframe
    if(xtals_idxs == None):
        np_slice = xtals_ds[:,:]
    else:
        np_slice = xtals_ds[:,xtals_idxs]
    slice_df = pd.DataFrame(data=np_slice)

    return pd.DataFrame(data=np_slice) 
   

def pullIOVtoDF(filename, corrections, xtals_idxs=None):
    '''
    A function to extract IOV data from an HDF5 file

    This function takes 3 arguments:
        filename -- the name of an IOV HDF5 file
        corrections -- a list of the corrections you would like (p1,p2,p3)
        xtals_idxs -- indices of crystals you would like
    It returns a dataframe of the corrections you specify
    '''
    #read in iov file
    hd_file = h5py.File(filename, 'r')
    
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

def pullSlimAndSkim(dst_file, oms_file, out_file=None, slim=True, max_xtals=2, xtal_sel='(eta_module == 4) & (phi_module == 1) & (LME == 38) & (PNA == 4)'):
    '''
    A function to combine OMS and DST HDF5 data and make a dataframe with selected crystals

    This function takes 6 arguments:
        dst_file -- Filename of the DST HDF5 to read
        oms_file -- Filename of the OMS HDF5 to read
        out_file -- If specified, the function writes a .csv at the end and returns nothing.
        slim -- Whether or not to only keep a few interesting columns (currently hardcoded)
        max_xtals -- A hard cut on how many crystals to keep
        xtal_sel -- This takes a string which slices into the ecalic.geom dataframe to pull crystal indices

    It returns nothing (saving the output) OR returns the resulting dataframe
    '''
    #This is an HdfLaser object, it also loads lumi info from the oms file
    data = HdfLaser(dst_file, hdf_run_info=oms_file, preload_fed=True)
    data.add_inst_lumi_info()
    t2 = data.t2 #table of the laser firing time per FED
    
    #This contains a dataframe which indexes the crystals across lots of different ways to slice the detector we use it to choose specific crystals
    from ecalic import geom
    
    #this grabs the text string and uses it as a pandas dataframe query since geom is literally an emap as a pandas.dataframe. It has many columns, including FED... and eta.
    print('Grabbing Crystal Indices')
    xtals = data.xtal_idx(xtal_sel)
    #This is a pandas dataframe with two columns (iov_idx and POSIX paths to iovs) in my case, they are all just paths to the same file. If your data was across multiple files, I guess this would tell the program where to find them.
    print('Grabbing IOV histories')
    iovs = data.iov_idx() #this just gets all of them
    #This gets the calibration values
    histories = data.xtal_history(iov_idx=iovs,xtal_idx=xtals) 
    #trim the xtal list to the max number requested
    if(max_xtals < len(xtals)):
        xtals = xtals[:max_xtals]
    print(xtals)
    slim_df_list = []
    for xtal in xtals:
        #loop through and grab the crystals
        print('Pulling history for xtal: ',xtal)
        crystal_history = histories.loc[:,xtal]
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

        #find FED
        fedId = geom.loc[xtal,'FED']
        #create t2 df as datetime
        t2_dt = t2.loc[:,fedId].apply(lambda x: pd.to_datetime(x, unit='s'))
        #merge in t2 for the FED
        dst_df = data.info.set_index(['run','seq']).join(t2_dt)
        
        #merge iov_idx into info df
        merge_df = pd.merge(iovdx, dst_df)
        
        #merge lumi_df with iov_df
        full_df = pd.merge(merge_df, iov_df, on='iov_idx')
        
        #just grab some of the interesting columns
        wanted_columns = ['iov_idx','date_x','calibration','inst_lumi', fedId]
        if(slim == True): 
            print('only taking colums: ')
            print(wanted_columns)
            slim_df = full_df.loc[:, wanted_columns]
        #or take the whole thing 
        else:
            slim_df = full_df

        #quick rename
        slim_df = slim_df.rename(columns={'date_x':'seq_datetime'})
        slim_df = slim_df.rename(columns={fedId:'laser_datetime'})

        #make cumulative inst_lumi column
        print(slim_df.columns)
        slim_df['int_inst_lumi'] = slim_df.inst_lumi.fillna(0.0).cumsum()

        slim_df_list.append(slim_df)

    #write it out or return the dataframe
    if not (out_file==None):
        for df, xtal in zip(slim_df_list, xtals):
            df.to_csv(out_file+'_'+str(xtal),index=True)
        return
    else:
        return slim_df_list, xtals

def load_month(month, year='18',folder='/home/rusack/evans908/FAIR/Lumi_Data'):
    '''
    A function to load a specified month of lumi section data from a .csv file

    This function takes 3 arguments:
        month -- the month of lumisections you would like
        year -- the year of lumi data to look in
        folder -- where to find the lumi data

    It returns a dataframe of lumisection data
    '''
    df = pd.read_csv(folder+'/'+'lumi_data_'+month+'_'+year+'.csv', skiprows=1) #first header row is not wanted
    df.drop(index=df.index[[-1,-2,-3]], inplace=True)   #last three are summary rows and you don't want them in the df
    #some of the last rows mess up the read in type
    df['E(GeV)'] = pd.to_numeric(df['E(GeV)'])
    df['delivered(/ub)'] = pd.to_numeric(df['delivered(/ub)'])
    #we want the time to be a timestamp
    df['time'] = pd.to_datetime(df['time'])
    
    return df

def load_year(year, folder='/home/rusack/evans908/FAIR/Lumi_Data', months=None):
    '''
    A function to load a year of lumi data from exisiting .csv files

    This function takes 3 arguments:
        year -- the year of lumisections to load
        folder -- where to find the lumi data
        months -- a double digit list of months you want to load from

    It returns a dataframe of all the selected lumisections
    '''
    if months == None:
        months = ['01', '02' ,'03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    dfs = []
    for month in months:
        print('loading: '+month)
        dfs.append(load_month(month, year, folder))

    df = pd.concat(dfs)
    #reset index post concat
    df = df.reset_index()
    df.drop(columns=['index'], inplace=True)
    #set time index
    df['ls_time'] = df.time
    df = df.set_index('time')
    #build two integrated columns    
    make_integrated_lumi(df)
    return df

def find_ls(laser_ts, bril_df): #find the lumi-section closest
    '''
    A function to find the nearest lumisection to a laser firing

    This function takes 2 arguments:
        laser_ts -- Timestamp of when the laser fired
        bril_df -- A dataframe of lumisections to search for a match

    It returns the lumi dataframe index that is closest
    '''
    idx = bril_df.index.get_loc(laser_ts, method='nearest')
    return idx 

def make_integrated_lumi(df):
    '''
    A function to make two integrated luminosity dataframes

    This function take 1 argument:
        df -- a lumi dataframe

    It returns nothing, instead it modifies the lumi dataframe inplace
    '''
    df['int_deliv_inv_ub'] = df['delivered(/ub)'].cumsum()
    df['int_record_inv_ub'] = df['recorded(/ub)'].cumsum()

def match_lumi_df(dst_df, lumi_df):
    '''
    A function to align in time a lumi and DST dataframe

    This function takes 2 arguments:
        dst_df -- A dataframe containing dst data
        lumi_df -- A dataframe containing lumi data

    It returns a lumi df with only the lumisections matching with laser firing
    '''
    #for each row, compare the datetime and find the index in the lumi_df
    lumi_idx = dst_df.apply(lambda x: find_ls(x.laser_datetime, lumi_df), axis=1)
    #return the slimmed lumi_df based on the indices
    return lumi_df.iloc[lumi_idx,:]

def combine_dfs(dst_df, iov_df, lumi_df):
    '''
    A function to stitch together a DST, IOV, and lumi dataframe

    This function takes 3 arguments:
        dst_df -- A dataframe containing DST data
        iov_df -- A dataframe containing IOV data
        lumi_df -- A dataframe containing lumi data

    It returns a combined dataframe
    '''
    matched_lumi_df = match_lumi_df(dst_df, lumi_df)
    matched_lumi_df = matched_lumi_df.reset_index()
    
    final_df = dst_df.join([iov_df, matched_lumi_df] )

    return final_df

