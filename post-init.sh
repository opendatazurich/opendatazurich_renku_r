#!/bin/sh
SCRIPT_DIR=$(dirname "$(realpath "$0")")

OLD_DIR=$(pwd)
# cd $SCRIPT_DIR

if [ -z "${PACKAGE_ID}" ]; then
    echo "PACKAGE_ID is not set. Using the default dataset."
    PACKAGE_ID="bau_hae_lima_zuordnung_adr_quartier_bzo16_bzo99_od5143"
fi

if [ -z "${RESOURCE_ID}" ]; then
    echo "RESOURCE_ID is not set. Using the first resource."
    RESOURCE_ID="NONE"
fi

python $SCRIPT_DIR/opendata/generate_starter_rmd.py "${PACKAGE_ID}"

# cd "$OLD_DIR"
