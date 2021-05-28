import df_builder as dfb
import pandas as pd


year = '2018'
yr = '18'
#this returns a multi-index dataframe, and the xtals that make it up. The first layer of index corresponds to the xtals returned
dst_dfs, xtals = dfb.pullSlimAndSkim('../ECAL_RADDAM_Data/'+year+'/dst.w447.hdf5',
                                     '../ECAL_RADDAM_Data/'+year+'/oms.hdf5', 
                                     out_file=None, 
                                     slim=False, 
                                     max_xtals=2,
                                     #xtal_sel='(eta_module == 4) & (phi_module == 1) & (LME == 38) & (PNA == 4)'
                                     xtal_sel='ieta==66'
                                     )
#load lumi_information
print('Loading lumi by month: ')
lumi_df = dfb.load_year(yr,'~/FAIR/Lumi_Data/'+year+'/') 
#for each xtal we want to grab it's iov data this also returns a multi-index dataframe
#for the highest precision, you want to line up the luminosity with the time each crystal is hit by the laser, which is different per FED. This could probably be done faster... but works fine for reasonable numbers of crystals
comb_dfs = []
for xtal, dst_df in zip(xtals, dst_dfs):
    print('Accessing IOV history for xtal: ',xtal)
    iov_df = dfb.pullIOVtoDF('../ECAL_RADDAM_Data/'+year+'/iov.w447.hdf5', ['p1','p2','p3'], xtal)
    print('Stitching...')
    comb_dfs.append(dfb.combine_dfs(dst_df, iov_df, lumi_df))

print('building final df')
#now we concatenate the dfs
final_df = pd.concat(comb_dfs, keys=xtals, axis=0)
#save it out
final_df.to_csv('slimmed_df_ieta66_'+year+'.csv', index=True)
