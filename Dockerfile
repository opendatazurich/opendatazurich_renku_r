FROM renku/renkulab-r:4.3.1-0.25.0

# Uncomment and adapt if code is to be included in the image
# COPY src /code/src

# Uncomment and adapt if your R or python packages require extra linux (ubuntu) software
# e.g. the following installs apt-utils and vim; each pkg on its own line, all lines
# except for the last end with backslash '\' to continue the RUN line
#
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libudunits2-dev \
    libproj-dev \
    libgdal-dev \
    libgeos-dev
RUN chown -R ${NB_USER} /home/${NB_USER}
USER ${NB_USER}

# install the python dependencies
COPY requirements.txt environment.yml /tmp/
RUN mamba env update -q -f /tmp/environment.yml && \
    /opt/conda/bin/pip install -r /tmp/requirements.txt --no-cache-dir && \
    mamba clean -y --all && \
    mamba env export -n "root" && \
    rm -rf ${HOME}/.renku/venv
RUN R -e "install.packages(c('units', 'sf', 'skimr'), repos='https://cloud.r-project.org')"

COPY startup.sh post-init.sh /usr/local/bin/
COPY opendata /usr/local/bin/opendata
COPY templates /usr/local/bin/templates
