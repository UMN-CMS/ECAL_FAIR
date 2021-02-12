import pandas as pd



def load_month(month, year='18',folder='/home/rusack/evans908/FAIR/Lumi_Data'):
    df = pd.read_csv(folder+'/'+'lumi_data_'+month+'_'+year+'.csv', skiprows=1) #first header row is not wanted
    df.drop(index=df.index[[-1,-2,-3]], inplace=True)   #last three are summary rows and you don't want them in the df
    #some of the last rows mess up the read in type
    df['E(GeV)'] = pd.to_numeric(df['E(GeV)'])
    df['delivered(/ub)'] = pd.to_numeric(df['delivered(/ub)'])
    #we want the time to be a timestamp
    df['time'] = pd.to_datetime(df['time'])
    
    return df

def load_year(year, folder='/home/rusack/evans908/FAIR/Lumi_Data', months=None):
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
    df = df.set_index('time')
    return df

def find_ls(laser_ts, bril_df): #find the lumi-section closest
    idx = bril_df.index.get_loc(laser_ts, method='nearest')
    return idx 
