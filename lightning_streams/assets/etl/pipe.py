#!/usr/bin/env python

import os
import shutil

from .extract import extract_s3
from .transform import transform_file
from .load import load_ene_tbl, load_geo_tbl


def load_tables(load_folder: str):
    """
    Batch & stream table loads.
    """
    ene_load_folder = os.path.join(load_folder, "ene")
    geo_load_folder = os.path.join(load_folder, "geo")

    if not os.path.exists(geo_load_folder):
        os.makedirs(geo_load_folder)
    geo_files = [
        s for s in os.listdir() if s.endswith("lat.csv") or s.endswith("lon.csv")
    ]

    try:
        for filename in geo_files:
            # Move files to be loaded
            shutil.move(filename, geo_load_folder)
    except Exception as e:
        print(f"Errors received while moving to {geo_load_folder}.")

    if not os.path.exists(ene_load_folder):
        os.makedirs(ene_load_folder)
    ene_files = [s for s in os.listdir() if s.endswith("ene.csv")]

    try:
        for filename in ene_files:
            # Move files to be loaded
            shutil.move(filename, ene_load_folder)
    except Exception as e:
        print(f"Errors received while moving to {ene_load_folder}.")

    load_geo_tbl(geo_load_folder)
    load_ene_tbl(ene_load_folder)
