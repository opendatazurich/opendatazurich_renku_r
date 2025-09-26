"""
A helper module with some useful functions for
interacting with the Open Data Swiss API.

"""

import geopandas as gpd
import json
import numpy as np
import pandas as pd
from owslib.wfs import WebFeatureService
import requests
import re
import time


import warnings
from IPython.display import display, HTML, Markdown

warnings.simplefilter(action="ignore", category=FutureWarning)

# Defaults

# Set constants for data provider and data API.
PROVIDER = "opendata.zh"
PROVIDER_LINK = "https://data.stadt-zuerich.ch"
BASELINK_DATAPORTAL = "https://data.stadt-zuerich.ch/dataset/"
CKAN_API_LINK = "https://data.stadt-zuerich.ch/api/3/action"
LANGUAGE = "de"

# Sort markdown table by this feature.
SORT_TABLE_BY = "title"

# Select keys in metadata for dataset and distributions.
KEYS_DATASET = [
    "dateLastUpdated",
    "maintainer",
    "maintainer_email",
    "metadata_created",
    "metadata_modified",
    "organization.name",
]
KEYS_DISTRIBUTIONS = [
    "package_id",
    "description",
    "dateLastUpdated",
    "license_id",
]

# Select relevant column names to reduce dataset.
REDUCED_FEATURESET = [
    "author",
    "author_email",
    "dateLastUpdated",
    "id",
    "maintainer",
    "maintainer_email",
    "metadata_created",
    "metadata_modified",
    "resources",
    "groups",
    "name",
    "language",
    "modified",
    "url",
    "identifier",
    "display_name.fr",
    "display_name.de",
    "display_name.en",
    "display_name.it",
    "organization.name",
    "organization.title.fr",
    "organization.title.de",
    "organization.title.en",
    "organization.title.it",
    "title",
    # The following are added for the codebooks.
    "contact",
    "distributions",
    "distribution_links",
    "metadata",
]


# Utility functions
def filter_tabular(full_df):
    """Filter to datasets that have a tabular data distribution"""
    df = full_df.copy()
    df.resources = df.resources.apply(has_tabular_distribution)
    df.dropna(subset=["resources"], inplace=True)
    return df


def filter_geo(full_df):
    """Filter to datasets that have a geo data distribution"""
    df = full_df.copy()
    df.resources = df.resources.apply(has_geo_distribution)
    df.dropna(subset=["resources"], inplace=True)
    return df


def get_dataset(url):
    """
    Return pandas df if url is parquet or csv file. Return None if not.
    """
    extension = url.rsplit(".", 1)[-1]
    if extension == "parquet":
        df = pd.read_parquet(url)
    elif extension == "csv":
        df = pd.read_csv(
            url,
            sep=",",
            on_bad_lines="warn",
            encoding_errors="ignore",
            low_memory=False,
        )
        # if dataframe only has one column or less the data is not comma separated, use ";" instead
        if df.shape[1] <= 1:
            df = pd.read_csv(
                url,
                sep=";",
                on_bad_lines="warn",
                encoding_errors="ignore",
                low_memory=False,
            )
            if df.shape[1] <= 1:
                print(
                    "The data wasn't imported properly. Very likely the correct separator couldn't be found.\nPlease check the dataset manually and adjust the code."
                )
    else:
        print("Cannot load data! Please provide an url with csv or parquet extension.")
        df = None
    return df


def has_tabular_distribution(dists):
    """Iterate over package resources and keep only tabular entries in list"""
    tabular_dists = [
        x
        for x in dists
        if x.get("format", "") == "CSV" or x.get("format", "") == "parquet"
    ]
    if tabular_dists != []:
        return tabular_dists
    else:
        return np.nan


def has_geo_distribution(dists):
    """Iterate over package resources and keep only geo entries in list"""
    geo_dists = [x for x in dists if x.get("format", "") == "WFS"]
    if geo_dists != []:
        return geo_dists
    else:
        return np.nan


def identifier_from_url(url):
    """
    Extracts the identifier from the url.
    """
    # Extract the identifier from the url
    identifier = re.search(r"\/([^\/\?]+)\?", url).group(1)
    return identifier


def url_to_geoportal_url(url):
    """
    Converts the url to a geoportal url.
    """
    # Extract the identifier from the url
    try:
        identifier = identifier_from_url(url)
        # Create the geoportal url
        geoportal_url = f"https://www.ogd.stadt-zuerich.ch/wfs/geoportal/{identifier}"
        return geoportal_url
    except AttributeError:
        if "/wfs/" in url:
            # If the url already contains /wfs/ return it
            return url
        else:
            # If the url does not contain /wfs/ return None
            print("Could not extract identifier from url.")
            return None


def geojson_layers_from_wfs(wfs):
    layers = list(wfs.contents.keys())
    return layers


def read_geojson_from_wfs(wfs, layer):
    response = wfs.getfeature(
        typename=layer, outputFormat="application/json; subtype=geojson"
    )
    return response.read()


# API
class OpenDataZH:
    def __init__(self):
        self.provider = PROVIDER
        self.provider_link = PROVIDER_LINK
        self.baselink_dataportal = BASELINK_DATAPORTAL
        self.ckan_api_link = CKAN_API_LINK
        self.language = LANGUAGE
        self.sort_table_by = SORT_TABLE_BY
        self.keys_dataset = KEYS_DATASET
        self.keys_distributions = KEYS_DISTRIBUTIONS
        self.reduced_featureset = REDUCED_FEATURESET

        self._full_package_list_df = None
        self._geo_package_list_df = None
        self._tabular_package_list_df = None

    def _get_full_package_list(self, limit=500, sleep=2):
        """Get full package list from CKAN API"""
        offset = 0
        frames = []
        while True:
            df = self._get_package_list_page(limit, offset)
            if df is None:
                break
            frames.append(df)
            offset += limit
            time.sleep(sleep)
        df = pd.concat(frames)
        df = df.set_index("name", drop=False).sort_index()
        self._full_package_list_df = df
        return df

    def _get_package_list_page(self, limit=500, offset=0):
        """Get a page of packages from CKAN API"""
        url = f"{self.ckan_api_link}/current_package_list_with_resources?limit={limit}&offset={offset}"
        res = requests.get(url)
        data = json.loads(res.content)
        if data["result"] == []:
            print("0 packages retrieved.")
            return None
        num_results = len(data["result"])
        print(f"{num_results} packages retrieved.")
        df = pd.DataFrame(pd.json_normalize(data["result"]))
        return df

    @property
    def full_package_list_df(self):
        if self._full_package_list_df is None:
            self._get_full_package_list()
        return self._full_package_list_df

    @property
    def tabular_package_list_df(self):
        if self._tabular_package_list_df is None:
            if self._full_package_list_df is None:
                self._get_full_package_list()
            self._tabular_package_list_df = filter_tabular(self._full_package_list_df)
        return self._tabular_package_list_df

    @property
    def geo_package_list_df(self):
        if self._geo_package_list_df is None:
            if self._full_package_list_df is None:
                self._get_full_package_list()
            self._geo_package_list_df = filter_geo(self._full_package_list_df)
        return self._geo_package_list_df

    def get_package(self, id=None, name=None):
        """Get a package from CKAN API"""
        if id is None and name is None:
            print("Please provide either an id or a name.")
            return None
        url = (
            f"{self.ckan_api_link}/package_show?id={id}"
            if id is not None
            else f"{self.ckan_api_link}/package_show?id={name}"
        )
        res = requests.get(url)
        data = json.loads(res.content)
        if not data["success"]:
            print(data.get("error", "No error message provided."))
            return None
        return OpenDataPackage(self, pd.json_normalize(data["result"]).iloc[0])


class OpenDataPackage:
    def __init__(self, odz, metadata):
        self.odz = odz
        self.metadata = metadata
        self.distributions = metadata.get("resources", [])
        self.distribution_links = [x.get("url") for x in self.distributions]
        self._resource_metadata_df = None
        self._tabular_resource_metadata_df = None
        self._geo_resource_metadata_df = None

    def display_metadata(self):
        display(
            HTML(
                f"<h2>Open Government Data, provided by <i>{self.odz.provider}</i></h2>"
                + f"<i>Generated Python starter code for data set with identifier</i> <b>{self.metadata['name']}</b>"
            )
        )

        display(HTML("<h2>Dataset</h2>" + f"<b>{self.metadata['title']}</b>"))

        display(HTML("<h2>Description</h2>"))
        display(Markdown(self.metadata["notes"]))
        display(Markdown(self.metadata["sszBemerkungen"]))

        display(HTML("<h2>Data set links</h2>"))
        display(
            HTML(
                f"<a href='{BASELINK_DATAPORTAL}{self.metadata['name']}'>Direct link by OpenDataZurich for dataset</a>"
            )
        )
        url = self.metadata.resources[0]["url"]
        display(HTML(f"<a href='{url}'>{url}</a>"))

        display(HTML("<h2>Metadata</h2>"))
        display_name = self.metadata["groups"][0]["display_name"]
        display_tags = [t["display_name"] for t in self.metadata["tags"]]
        display(
            Markdown(
                f"* **Publisher** {self.metadata['author']}\n"
                + f"* **Maintainer** {self.metadata['maintainer']}\n"
                + f"* **Maintainer email** {self.metadata['maintainer_email']}\n"
                + f"* **Keywords** {display_name}\n"
                + f"* **Tags** {display_tags}\n"
                + f"* **Metadata created** {self.metadata['metadata_created']}\n"
                + f"* **Metadata modified** {self.metadata['metadata_modified']}\n"
            )
        )

    def display_resource_summary(self):
        display(
            HTML(
                "<h2>Resources</h2>"
                + f"<b>{len(self.resource_metadata_df)} resource(s) found in this dataset.</b>"
            )
        )
        summary_ser = self.resource_metadata_df.groupby("format").count()["url"]
        summary_ser.name = "resources by type"
        display(HTML(summary_ser.to_frame().to_html()))

    @property
    def geo_resource_metadata_df(self):
        if self._geo_resource_metadata_df is None:
            self._geo_resource_metadata_df = self.resource_metadata_df[
                self.resource_metadata_df["format"].isin(["WFS", "JSON"])
            ]
        return self._geo_resource_metadata_df

    def geo_resource(self, index=0, id=None):
        """Find a resource by id, if provided, otherwise by index"""
        if id is not None:
            metadata = self.geo_resource_metadata_df[
                self.geo_resource_metadata_df["id"] == id
            ].iloc[0]
        else:
            metadata = self.geo_resource_metadata_df.iloc[index]
        return OpenDataGeoResource(self, index, metadata)

    @property
    def resource_metadata_df(self):
        if self._resource_metadata_df is None:
            self._resource_metadata_df = pd.DataFrame(self.metadata["resources"])
        return self._resource_metadata_df

    @property
    def tabular_resource_metadata_df(self):
        if self._tabular_resource_metadata_df is None:
            self._tabular_resource_metadata_df = self.resource_metadata_df[
                self.resource_metadata_df["format"].isin(["CSV", "parquet"])
            ]
        return self._tabular_resource_metadata_df

    def tabular_resource(self, index=0, id=None):
        """Find a resource by id, if provided, otherwise by index"""
        if id is not None:
            metadata = self.tabular_resource_metadata_df[
                self.tabular_resource_metadata_df["id"] == id
            ].iloc[0]
        else:
            metadata = self.tabular_resource_metadata_df.iloc[index]
        return OpenDataTabularResource(self, index, metadata)


class OpenDataGeoResource:
    def __init__(self, package, index, metadata):
        self.package = package
        self.index = index
        self.metadata = metadata
        self._df = None
        self._layers = None
        self._wfs = None

    def display_metadata(self):
        display(
            Markdown(
                f"* **name** {self.metadata['name']}\n"
                f"* **format** {self.metadata['format']}\n"
                f"* **url** {self.metadata['url']}\n"
                f"* **id** {self.metadata['id']}\n"
                f"* **resource_type** {self.metadata['resource_type']}\n"
                f"* **package_id** {self.metadata['package_id']}\n"
            )
        )

    def layer_df(self, layer):
        """Return a geopandas data frame of the layer."""
        # Read the geojson from the WFS
        response = read_geojson_from_wfs(self.wfs, layer)
        return gpd.read_file(response)

    @property
    def df(self):
        """Return a geopandas data frame of first layer."""
        if self._df is None:
            self._df = self.layer_df(self.layers[0])
        return self._df

    @property
    def layers(self):
        if self._layers is None:
            self._layers = geojson_layers_from_wfs(self.wfs)
        return self._layers

    @property
    def wfs(self):
        if self._wfs is None:
            # Remove any query parameters from the url
            geoportal_url = url_to_geoportal_url(self.metadata["url"])
            self._wfs = WebFeatureService(geoportal_url, version="1.1.0")
        return self._wfs


class OpenDataTabularResource:
    def __init__(self, package, index, metadata):
        self.package = package
        self.index = index
        self.metadata = metadata
        self._df = None

    def display_metadata(self):
        display(
            Markdown(
                f"* **name** {self.metadata['name']}\n"
                f"* **filename** {self.metadata['filename']}\n"
                f"* **format** {self.metadata['format']}\n"
                f"* **url** {self.metadata['url']}\n"
                f"* **id** {self.metadata['id']}\n"
                f"* **resource_type** {self.metadata['resource_type']}\n"
                f"* **package_id** {self.metadata['package_id']}\n"
            )
        )

    @property
    def df(self):
        if self._df is None:
            self._df = get_dataset(self.metadata["url"])
        return self._df
