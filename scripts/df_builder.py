import pandas as pd
import lumi_util as luti

def dst_iov_comb(dst_df, iov_dfs):


def match_lumi_df(dst_df, lumi_df):
    #for each row, compare the datetime and find the index in the lumi_df
    lumi_idx = dst_df.apply(lambda x: find_ls(x.datetime, lumi_df), axis=1)
    #return the slimmed lumi_df based on the indices
    return lumi_df.iloc[lumi_idx,:]

