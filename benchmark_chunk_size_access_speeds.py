#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  3 14:36:51 2023

@author: lukem
"""

import xarray as xr
import time
import pandas as pd

# Define the URL of the THREDDS server and the dataset path for files to check
thredds_urls = [
]
output_csv_filepath = '' # absolute path to the CSV file you want to write to

# Function to open the dataset and measure time
def open_and_measure_time(url):
    start_time = time.time()
    ds = xr.open_dataset(url)
    end_time = time.time()
    open_time = end_time - start_time
    return ds, round(open_time,2)

def region(ds, T, Y, X):
    # T, Y, X are range objects
       
    #test = ds.B9[8, ymin:ymax, xmin:xmax]
    start_time = time.time()
    test = ds.B9.isel(time=T, y=Y, x=X)
    std = test.std()
    end_time = time.time()
    
    extract_time = end_time - start_time
    return round(extract_time, 2)

dic = {}
for ii in range(10):
    print('iteration:',ii)
    
    df = pd.DataFrame()
    for thredds_url in thredds_urls:
        
        chunk_size = thredds_url.split('-CS_')[1]
        print(chunk_size)
        times = []
        tests = []
        
        # Open the dataset and measure the time taken
        ds, open_time = open_and_measure_time(thredds_url)
        times.append(open_time)
        tests.append('Opening file')
        max_x = len(ds.x)-1
        max_y = len(ds.y)-1
        max_t = len(ds.time)
        
        # Time slices
        X = slice(0,max_x)
        Y = slice(0,max_y)
        step = round(max_t / 4)
        T_list = list(range(0, max_t + 1, step))
        for T in T_list:
            extract_time = region(ds, T, X, Y)
            times.append(extract_time)
            tests.append(f'Extracting data x={X}, Y={Y}, t={T}')
        
        # Small time slice
        X = slice(round(max_x/3),round(max_x/2))
        Y = slice(round(max_y/3),round(max_y/2))
            
        step = round(max_t / 4)
        T_list = list(range(0, max_t + 1, step))
        for T in T_list:
            extract_time = region(ds, T, X, Y)
            times.append(extract_time)
            tests.append(f'Extracting data x={X}, Y={Y}, t={T}')
        
        # Time series constant Y
        X = slice(0,max_x)
        T = slice(0,max_t)
            
        step = round(max_y / 4)
        Y_list = list(range(0, max_y + 1, step))
        for Y in Y_list:
            extract_time = region(ds, T, X, Y)
            times.append(extract_time)
            tests.append(f'Extracting data x={X}, Y={Y}, t={T}')
        
        # Time series constant X
        Y = slice(0,max_y)
        T = slice(0,max_t)
            
        step = round(max_x / 4)
        X_list = list(range(0, max_x + 1, step))
        for X in X_list:
            extract_time = region(ds, T, X, Y)
            times.append(extract_time)
            tests.append(f'Extracting data x={X}, Y={Y}, t={T}')
        
        df[chunk_size] = times

    df['test'] = tests
    dic[ii] = df

# Extract the first DataFrame to use as a template for the structure
template_df = list(dic.values())[0][['32_32', '64_64', '91_99']].copy()

# Initialize the average DataFrame with zeros
average_df = pd.DataFrame(0, index=template_df.index, columns=template_df.columns)

# Iterate through the DataFrames in the dictionary and add their values to the average DataFrame
for df in dic.values():
    average_df += df[['32_32', '64_64', '91_99']]

# Divide the sum by the number of DataFrames to get the average
average_df /= len(dic)
average_df['test'] = df['test']
average_df.to_csv(output_csv_filepath, index=False)
print(average_df)