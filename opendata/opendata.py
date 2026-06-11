"""
A helper module with some useful functions for
interacting with the Open Data Swiss API.

"""

import json
import pandas as pd
import requests


# Defaults

# Set constants for data provider and data API.
PROVIDER = "opendata.zh"
PROVIDER_LINK = "https://data.stadt-zuerich.ch"
BASELINK_DATAPORTAL = "https://data.stadt-zuerich.ch/dataset/"
CKAN_API_LINK = "https://data.stadt-zuerich.ch/api/3/action"


# API
class OpenDataZurich:
    def __init__(self, user_agent=None):
        self.provider = PROVIDER
        self.provider_link = PROVIDER_LINK
        self.baselink_dataportal = BASELINK_DATAPORTAL
        self.ckan_api_link = CKAN_API_LINK

        self.session = requests.Session()
        if user_agent:
            self.session.headers.update({"User-Agent": user_agent})

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
        res = self.session.get(url)
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
