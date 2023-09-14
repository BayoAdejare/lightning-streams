#!/usr/bin/env python

import os
import shutil

import netCDF4 as nc
import pandas as pd

from pathlib import Path

def transform_file(extract_folder: str, transform_folder: str, filename: str, context: str=None) -> pd.DataFrame: 
    """
    Convert GOES netCDF files into csv
    """
    file_conn = Path(os.path.join(extract_folder, filename))
    energy_filename = file_conn.with_suffix('').with_suffix('.ene.csv') # energy file
    lat_filename = file_conn.with_suffix('').with_suffix('.lat.csv') # latitude file
    lon_filename = file_conn.with_suffix('').with_suffix('.lon.csv') # longitude file
    # create dataset
    glm =  nc.Dataset(file_conn, mode='r')
    flash_lat = glm.variables['flash_lat'][:]
    flash_lon = glm.variables['flash_lon'][:]
    flash_time = glm.variables['flash_time_offset_of_first_event']
    flash_energy = glm.variables['flash_energy'][:]
    dtime = nc.num2date(flash_time[:],flash_time.units)
    # flatten multi-dimensional data into series        
    flash_energy_ts = pd.Series(flash_energy, index=dtime)
    flash_lat_ts = pd.Series(flash_lat, index=dtime)
    flash_lon_ts = pd.Series(flash_lon, index=dtime)
    # headers 
    with open(energy_filename, 'w') as glm_file:
        glm_file.write('ts_date,energy\n')
    # write to csv
    flash_energy_ts.to_csv(energy_filename, index=True, header=False, mode='a')
    # headers 
    with open(lat_filename, 'w') as glm_file:
        glm_file.write('ts_date,lat\n')
    # write to csv
    flash_lat_ts.to_csv(lat_filename, index=True, header=False, mode='a')
    with open(lon_filename, 'w') as glm_file:
        glm_file.write('ts_date,lon\n')
    # write to csv
    flash_lon_ts.to_csv(lon_filename, index=True, header=False, mode='a')
    # move files
    shutil.move(energy_filename, transform_folder) 
    shutil.move(lat_filename, transform_folder) 
    shutil.move(lon_filename, transform_folder) 
    # list converted files
    df_transform = pd.DataFrame(os.listdir())
    return df_transform
