#!/bin/sh
SCRIPT_DIR=$(dirname "$(realpath "$0")")

pushd . > /dev/null
cd $SCRIPT_DIR

if [ -z "${PACKAGE_ID}" ]; then
    echo "PACKAGE_ID is not set. Using the default dataset."
    # papermill templates/TemplateCsv.ipynb OpenData.ipynb
    exit 0
fi

if [ -z "${RESOURCE_ID}" ]; then
    echo "RESOURCE_ID is not set. Using the first resource."
    RESOURCE_ID="NONE"
fi

dataset_type=$(python src/get_dataset_type.py "${PACKAGE_ID}")
if [ "${dataset_type}" = "csv" ]; then
    echo "Using CSV dataset."
    # papermill templates/TemplateCsv.ipynb OpenData.ipynb -p package_id "${PACKAGE_ID}" -p resource_id "${RESOURCE_ID}"
elif [ "${dataset_type}" = "geo" ]; then
    echo "Using Geo dataset."
    # papermill templates/TemplateGeo.ipynb OpenData.ipynb -p package_id "${PACKAGE_ID}" -p resource_id "${RESOURCE_ID}"
else
    echo "Unknown dataset type: ${dataset_type}. Defaulting to CSV."
    # papermill templates/TemplateCsv.ipynb OpenData.ipynb -p package_id "${PACKAGE_ID}"  -p resource_id "${RESOURCE_ID}"
fi

popd > /dev/null
