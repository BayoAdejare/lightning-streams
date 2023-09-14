from setuptools import find_packages, setup

setup(
    name="lightning_streams",
    version="0.0.1",
    packages=find_packages(exclude=["lightning_streams_tests"]),
    install_requires=[
        ## app dependencies
        "dagster-webserver==1.4.12", 
        "dagster==1.4.12",          
        "dagster-duckdb==0.20.12",
        "dagster-duckdb-pyspark==0.20.12",
        "netCDF4==1.6.4",
        "pandas==2.0",
        "boto3==1.28.44",
        "botocore==1.31.44",
        "scikit-learn==1.3.0",
        "pyspark==3.4.1",
        ## notebook dependencies
        # "geopandas==0.13.2",
        # "matplotlib==3.7.2",
        # "seaborn==0.12.2",
    ],
    author='Bayo Adejare',
    author_email='bayo.adejare@gmail.com',
    extras_requires={"dev": ["dagit", "pytest==7.4.2"]},
)