# OpenDataZurich data in Renkulab (R)

This repository is an example of how to work with OpenDataZurich data with R in Renkulab. The Dockerfile defines a session launcher with the environment correctly set up.

## User-Agent

All outgoing HTTP requests identify themselves with a custom `User-Agent` so traffic from this repo is traceable in the OpenDataZurich access logs:

- Generator (Python, when fetching CKAN metadata to populate the templates): `OpenDataZurich-Renku/1.0 (lang=python; +https://github.com/opendatazurich/opendatazurich_renku_r)`
- Generated R notebooks (when reading data via `readr`/`arrow`/`sf`/`httr2`): `OpenDataZurich-Renku/1.0 (lang=r; +https://github.com/opendatazurich/opendatazurich_renku_r)`

In the R notebooks the User-Agent is applied across all HTTP layers used: base R / libcurl via `options(HTTPUserAgent = ...)`, GDAL (used by `sf::st_read`) via `Sys.setenv(GDAL_HTTP_USERAGENT = ...)`, and httr2 via `req_user_agent(...)`.


# Local development

Run container with tabular data 
RStudio should be available at http://localhost:8888/rstudio

```bash
docker run -it --rm \
  -p 8888:8888 \
  -v "$(pwd):/home/rstudio/work" \
  -e NB_UID=1000 \
  -e NB_GID=1000 \
  -e PACKAGE_ID="politik_abstimmungen_seit1933" \
  -e RESOURCE_ID="3e87b102-f19c-47f4-ab50-a679b51cf77e" \
  opendatazurich_renku_r \
  /bin/bash -c "bash /usr/local/bin/startup.sh"
```

Run container with geo data 
RStudio should be available at http://localhost:8888/rstudio

```bash
docker run -it --rm \
  -p 8888:8888 \
  -v "$(pwd):/home/rstudio/work" \
  -e NB_UID=1000 \
  -e NB_GID=1000 \
  -e PACKAGE_ID="geo_oeffentlich_zugaengliche_parkplaetze_dav" \
  -e RESOURCE_ID="9d4f3dfc-e998-4c23-ba4e-6dde97a4ee16" \
  opendatazurich_renku_r \
  /bin/bash -c "bash /usr/local/bin/startup.sh"
```