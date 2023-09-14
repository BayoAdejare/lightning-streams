import pandas as pd

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


def preprocess(
    lat: pd.DataFrame, lon: pd.DataFrame, context: str = None
) -> pd.DataFrame:
    """Preprocess the data"""
    # drop file name
    lat.drop(lat.columns[[2]], axis=1, inplace=True)
    lon.drop(lon.columns[[2]], axis=1, inplace=True)
    # remove duplicates
    lat.drop_duplicates(subset=["ts_date"], inplace=True)
    lon.drop_duplicates(subset=["ts_date"], inplace=True)
    # join data
    geo_df = lon.join(lat.set_index("ts_date"), on="ts_date")

    return geo_df


def kmeans_model(data: pd.DataFrame, num_clusters: int, context: str = None):
    """
    Fit data to kmeans cluster algorithm.
    """
    X = data.loc[:, ["lon", "lat"]]

    kmeans_kwargs = {
        "init": "k-means++",
        "n_init": 10,
        "max_iter": 100,
        "random_state": 60,
    }

    kmeans = KMeans(n_clusters=num_clusters, **kmeans_kwargs)
    X["Cluster"] = kmeans.fit_predict(X)
    X["Cluster"] = X["Cluster"].astype("category")
    return X


def sil_evaluation(data: pd.DataFrame, context: str = None):
    """
    Evaluate the k-means silhouette coefficient.
    """
    data = data.loc[:, ["lon", "lat"]]

    kmeans_kwargs = {
        "init": "k-means++",
        "n_init": 10,
        "max_iter": 50,
        "random_state": 60,
    }

    # holds the silhouette coefficients for each k
    silhouette_coefficients = {}

    # start at 2 clusters for silhouette coefficient
    for k in range(2, 24):
        kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
        kmeans.fit(data)
        score = silhouette_score(data, kmeans.labels_)
        silhouette_coefficients[k] = score

    sil_df = pd.DataFrame(
        list(silhouette_coefficients.items()), columns=["k", "silhouette_coefficient"]
    )

    return sil_df


def elb_evaluation(data: pd.DataFrame, context: str = None):
    """
    Evaluate the k-means elbow method, sum of squared error.
    """

    kmeans_kwargs = {
        "init": "k-means++",
        "n_init": 10,
        "max_iter": 50,
        "random_state": 60,
    }

    # A list holds the sum of squared distance for each k
    elb_sse = []

    # Return SSE for each k
    for k in range(1, 24):
        kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
        kmeans.fit(data)
        elb_sse.append(kmeans.inertia_)

    return elb_sse
