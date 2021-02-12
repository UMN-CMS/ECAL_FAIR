import df_builder as dfb
import iovs_to_pd_df as itpd
import lumi_util as luti
import slim_and_skim as sas

dst_df, xtals = sas.pullSlimAndSkim('../ECAL_RADDAM_Data/2018/dst.w447.hdf5', '../ECAL_RADDAM_Data/2018/oms.hdf5')
iov_df = itpd.pullIOVtoDF('../ECAL_RADDAM_Data/2018/iov.w447.hdf5', ['p1','p2','p3'], xtals[0])
lumi_df = luti.load_year('18')

final_df = dfb.combine_dfs(dst_df, iov_df, lumi_df)
