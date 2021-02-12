import pandas as pd
import lumi_util as luti

def match_lumi_df(dst_df, lumi_df):
    #for each row, compare the datetime and find the index in the lumi_df
    lumi_idx = dst_df.apply(lambda x: luti.find_ls(x.datetime, lumi_df), axis=1)
    #return the slimmed lumi_df based on the indices
    return lumi_df.iloc[lumi_idx,:]

def combine_dfs(dst_df, iov_df, lumi_df):
    matched_lumi_df = match_lumi_df(dst_df, lumi_df)
    matched_lumi_df = matched_lumi_df.reset_index()
    
    final_df = dst_df.join([iov_df, matched_lumi_df] )

    return final_df
