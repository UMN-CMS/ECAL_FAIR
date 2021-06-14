import os
import re
import sys
import zipfile
from collections import UserDict

import numpy as np
import pandas as pd
import requests

#utilities made by me##############################################################


def scale_shrinker(pd_series, floor, take_log=True, calc=True):
    """
    Takes in a pandas series of positive numbers. 
    It truncates data below a chosen order of magnitude (wrt the max) to 0.0
    and optionally takes the log10 of the remaining data
    If calc = True, it calculates the floor as an order of magnitude below the max
    otherwise, it just takes the floor value given as the floor
    """
    def cutfloor(x, floor):
        '''return value if x > floor, return 0.0 otherwise'''
        if x > floor:
            return x
        else:
            return 0.0
    #we don't want to touch the original series
    new_series = pd_series.copy()
    #calculate floor if requested
    if(calc):
        floor = 10.0 ** (-1.0 * float(floor))
        max_val = pd_series.max()
        floor = max_val * floor
        if(take_log):
            floor = np.log10(floor)

    #log space
    if(take_log):
        new_series = new_series.apply(np.log10)
    
    #or just drop values
    new_series = new_series.apply(lambda x: cutfloor(x, floor))
    
    return new_series, floor
    
#############################################################################################################
#MIT License
#
#Copyright (c) 2020 Francesca Lazzeri
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE FOLLOWING UTILITIES COME FROM F. Lazzeri @ https://github.com/FrancescaLazzeri/Machine-Learning-for-Time-Series-Forecasting
#

def mape(predictions, actuals):
    """Mean absolute percentage error"""
    return ((predictions - actuals).abs() / actuals).mean()


def create_evaluation_df(predictions, test_inputs, H, scaler):
    """Create a data frame for easy evaluation"""
    eval_df = pd.DataFrame(
        predictions, columns=["t+" + str(t) for t in range(1, H + 1)]
    )
    eval_df["timestamp"] = test_inputs.dataframe.index
    eval_df = pd.melt(
        eval_df, id_vars="timestamp", value_name="prediction", var_name="h"
    )
    eval_df["actual"] = np.transpose(test_inputs["target"]).ravel()
    eval_df[["prediction", "actual"]] = scaler.inverse_transform(
        eval_df[["prediction", "actual"]]
    )
    return eval_df


class TimeSeriesTensor(UserDict):
    """A dictionary of tensors for input into the RNN model.
    Use this class to:
      1. Shift the values of the time series to create a Pandas dataframe containing all the data
         for a single training example
      2. Discard any samples with missing values
      3. Transform this Pandas dataframe into a numpy array of shape
         (samples, time steps, features) for input into Keras
    The class takes the following parameters:
       - **dataset**: original time series
       - **target** name of the target column
       - **H**: the forecast horizon
       - **tensor_structures**: a dictionary discribing the tensor structure of the form
             { 'tensor_name' : (range(max_backward_shift, max_forward_shift), [feature, feature, ...] ) }
             if features are non-sequential and should not be shifted, use the form
             { 'tensor_name' : (None, [feature, feature, ...])}
       - **freq**: time series frequency (default 'H' - hourly)
       - **drop_incomplete**: (Boolean) whether to drop incomplete samples (default True)
    """

    def __init__(
        self, dataset, target, H, tensor_structure, freq="H", drop_incomplete=True
    ):
        self.dataset = dataset
        self.target = target
        self.tensor_structure = tensor_structure
        self.tensor_names = list(tensor_structure.keys())

        self.dataframe = self._shift_data(H, freq, drop_incomplete)
        self.data = self._df2tensors(self.dataframe)

    def _shift_data(self, H, freq, drop_incomplete):

        # Use the tensor_structures definitions to shift the features in the original dataset.
        # The result is a Pandas dataframe with multi-index columns in the hierarchy
        #     tensor - the name of the input tensor
        #     feature - the input feature to be shifted
        #     time step - the time step for the RNN in which the data is input. These labels
        #         are centred on time t. the forecast creation time
        df = self.dataset.copy()

        idx_tuples = []
        for t in range(1, H + 1):
            df["t+" + str(t)] = df[self.target].shift(t * -1, freq=freq)
            idx_tuples.append(("target", "y", "t+" + str(t)))

        for name, structure in self.tensor_structure.items():
            rng = structure[0]
            dataset_cols = structure[1]

            for col in dataset_cols:

                # do not shift non-sequential 'static' features
                if rng is None:
                    df["context_" + col] = df[col]
                    idx_tuples.append((name, col, "static"))

                else:
                    for t in rng:
                        sign = "+" if t > 0 else ""
                        shift = str(t) if t != 0 else ""
                        period = "t" + sign + shift
                        shifted_col = name + "_" + col + "_" + period
                        df[shifted_col] = df[col].shift(t * -1, freq=freq)
                        idx_tuples.append((name, col, period))

        df = df.drop(self.dataset.columns, axis=1)
        idx = pd.MultiIndex.from_tuples(
            idx_tuples, names=["tensor", "feature", "time step"]
        )
        df.columns = idx

        if drop_incomplete:
            df = df.dropna(how="any")

        return df

    def _df2tensors(self, dataframe):

        # Transform the shifted Pandas dataframe into the multidimensional numpy arrays. These
        # arrays can be used to input into the keras model and can be accessed by tensor name.
        # For example, for a TimeSeriesTensor object named "model_inputs" and a tensor named
        # "target", the input tensor can be acccessed with model_inputs['target']

        inputs = {}
        y = dataframe["target"]
        y = y.to_numpy()
        inputs["target"] = y

        for name, structure in self.tensor_structure.items():
            rng = structure[0]
            cols = structure[1]
            tensor = dataframe[name][cols].to_numpy()
            if rng is None:
                tensor = tensor.reshape(tensor.shape[0], len(cols))
            else:
                tensor = tensor.reshape(tensor.shape[0], len(cols), len(rng))
                tensor = np.transpose(tensor, axes=[0, 2, 1])
            inputs[name] = tensor

        return inputs

    def subset_data(self, new_dataframe):

        # Use this function to recreate the input tensors if the shifted dataframe
        # has been filtered.

        self.dataframe = new_dataframe
        self.data = self._df2tensors(self.dataframe) 
