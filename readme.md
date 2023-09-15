# Lightning Streams

An example of a simple `stream and batch query` made by implementing PySpark, python API of [Apache Spark™](https://spark.apache.org/), queries on a Lightning flash dataset collected from NOAA's GLM.
Uses [Apache Parquet](https://parquet.apache.org/) file format as the storage backend and [Dagster Software-Defined Assets](https://docs.dagster.io/concepts/assets/software-defined-assets) to orchestrate the batch/stream processing pipeline.

Blog post: Coming soon!

|<a href="img/main_tech_stack.png" align="center"><img src="img/main_tech_stack.png" alt="Technologies used and respective logos" width="800px"/></a>
|:--:|
|Dagster + PySpark + Parquet|

## Installation

First make sure, you have the requirements installed, this can be installed from the project directory via pip's setup command:

`pip install . # =< python3.11 `

## Quick Start

Run the command to start the dagster orchestration framework: 

`dagster dev # Start dagster daemon and dagit ui`

The dagster daemon is required to start the scheduling, from the dagit ui, you can run and monitor the data assets.

## ETL Pipeline

ETL pipe data assets:

+ `Source`: **extracts** NOAA GOES-R GLM file datasets from AWS s3 bucket. 
+ `Transformations`: **transforms** dataset into time series csv.
+ `Sink`: **loads** dataset to persistant storage.

Sink loading process refactored to use `pyspark` (batch and structured streaming queries) and parquet as the storage backend.

|<a href="img/pipeline/etl_pipe.png" align="center"><img src="img/pipeline/etl_pipe.png" alt="ETL Data assets" width="300px"/></a>
|:--:|
|ETL Data asset group|

## Clustering Pipeline

Blog post: [Exploratory Data Analysis with Lightning Streaming Pipeline](https://medium.com/@adebayoadejare/exploratory-data-analysis-with-lightning-clustering-pipeline-6a2bca17d0d3)


|<a href="img/pipeline/eda_sda_job.png" align="center"><img src="img/pipeline/eda_sda_job.png" alt="Lightning clustering pipeline Illustration" width="400px"/></a>
|:--:|
|Materializing Lightning clustering pipeline|

### Data Ingestion

Ingests the data needed based on specified time window: start and end dates.

#### Data Assets

+ `ingestor`: Composed of `extract`, `transform`, and `load` data assets.
+ `extract`: downloads [NOAA GOES-R GLM](https://www.goes-r.gov/spacesegment/glm.html) netCDF files from AWS s3 bucket
+ `transform`: converts GLM netCDF into time and geo series CSVs 
+ `load`: loads CSVs to a local backend, persistant duckdb

### Cluster Analysis

Performs grouping of the ingested data by implementing K-Means clustering algorithm.

|<a href="img/cluster_process.png" align="center"><img src="img/cluster_process.png" alt="An example clustering of flash data points" width="800px"/></a>
|:--:|
|Visual of clustering process|

#### Data Assets

+ `preprocessor`: prepares the data for cluster model, clean and normalize the data.
+ `kmeans_cluster`: fits the data to an implementation of k-means cluster algorithm.
+ `silhouette_evaluator`: evaluates the choice of 'k' clusters by calculating the silhouette coefficient for each k in defined range.
+ `elbow_evaluator`: evaluates the choice of 'k' clusters by calculating the sum of the squared distance for each k in defined range.

|<a href="img/pipeline/eda_sda_pipe.png" align="center"><img src="img/pipeline/eda_sda_pipe.png" alt="Display of clustering materialized assets" width="400px"/></a>
|:--:|
|Displaying Clusering analysis data assets|



|<a href="./img/sample_lightning_clusters.gif" align="center"><img src="./img/sample_lightning_clusters.gif" alt="An example clustering of flash data points" width="600px"/></a>
|:--:|
|Lightning clustering map|


## Testing

Use the following command to run tests:

`pytest`

## License

[Apache 2.0 License](LICENSE)
