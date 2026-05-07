# OpenDataZurich data in Renkulab (R)

This repository is an example of how to work with OpenDataZurich data with R in Renkulab. The Dockerfile defines a session launcher with the environment correctly set up.

## User-Agent

All outgoing HTTP requests identify themselves with a custom `User-Agent` so traffic from this repo is traceable in the OpenDataZurich access logs:

- Generator (Python, when fetching CKAN metadata to populate the templates): `OpenDataZurich-Renku/1.0 (lang=python; +https://github.com/opendatazurich/opendatazurich_renku_r)`
- Generated R notebooks (when reading data via `readr`/`arrow`/`sf`/`httr2`): `OpenDataZurich-Renku/1.0 (lang=r; +https://github.com/opendatazurich/opendatazurich_renku_r)`

In the R notebooks the User-Agent is applied across all HTTP layers used: base R / libcurl via `options(HTTPUserAgent = ...)`, GDAL (used by `sf::st_read`) via `Sys.setenv(GDAL_HTTP_USERAGENT = ...)`, and httr2 via `req_user_agent(...)`.
