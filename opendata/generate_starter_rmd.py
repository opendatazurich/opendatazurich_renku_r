# %%
# IMPORTS -------------------------------------------------------------------- #

from datetime import datetime
import pandas as pd
import re
import sys

from opendata import OpenDataZH
import os

MODULE_PATH = os.path.abspath(__file__)
MODULE_DIR = os.path.dirname(MODULE_PATH)

# %%
# CONSTANTS ------------------------------------------------------------------ #

# Set constants for data provider and data API.
PROVIDER = "OpenDataZurich"
BASELINK_DATAPORTAL = "https://data.stadt-zuerich.ch/dataset/"

# Set local folders and file names.
# Switch to relative path when running this file directly
# TEMPLATE_FOLDER = "../templates/"
TEMPLATE_FOLDER = os.path.join(os.path.dirname(MODULE_DIR), "templates")
TEMPLATE_RMARKDOWN = "template_rmarkdown.Rmd"
TEMPLATE_RMARKDOWN_GEO = "template_rmarkdown_geo.Rmd"

TODAY_DATE = datetime.today().strftime("%Y-%m-%d")
TODAY_DATETIME = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

# Select keys in metadata for dataset and distributions.
KEYS_DATASET = [
    "publisher",
    "maintainer",
    "maintainer_email",
    "keywords",
    "tags",
    "metadata_created",
    "metadata_modified",
]


PREFIX_RESOURCE_COLS = "resources_"
RESOURCE_COLS_TO_KEEP = [
    "name",
    "filename",
    "format",
    "url",
    "id",
    "resource_type",
    "package_id",
]


# %%
# FUNCTIONS ------------------------------------------------------------------ #
def dataset_to_resource(
    all_packages,
    prefix_resource_cols=PREFIX_RESOURCE_COLS,
    resource_cols_to_keep=RESOURCE_COLS_TO_KEEP,
):
    """
    Takes pandas df with all datasets (one row for each dataset).
    Column "resources" must contain json info for each resource like:
    [{'cache_last_updated': None, 'cache_url': None},...]
    Json fields in resource get a prefix: prefix_resource_cols
    This function explodes the df, so that each row in the output represents one resource.
    """
    print("Explode dataset to resource level")
    # explode every resource in one row
    all_packages_exploded = all_packages.explode("resources")
    # json to columns and only keep the selected
    resources_exploded_df = pd.json_normalize(all_packages_exploded["resources"])
    resources_cols_to_keep_in_df = [
        col for col in resource_cols_to_keep if col in resources_exploded_df.columns
    ]
    resource_cols = resources_exploded_df[resources_cols_to_keep_in_df]

    # add prefix, to avoid already existing columns
    resource_cols = resource_cols.add_prefix(prefix_resource_cols)
    # merge data from package/dataset
    merged = resource_cols.merge(
        all_packages,
        how="left",
        left_on=PREFIX_RESOURCE_COLS + "package_id",
        right_on="id",
    )

    # reset index, because later functions will need unique indices
    merged = merged.reset_index(drop=True)
    return merged


def filter_resources(df, desired_formats=["table_data", "geo_data"]):
    """
    Filter df with resources for desired_formats (e.g. csv).
    Be aware that the filtered column has to match the prefix defined in dataset_to_resource.
    returns a dict with desired_formats as keys and filtered dataframes as values

    """
    print("Filter data by:", desired_formats)
    # set col value for table data
    if "table_data" in desired_formats:
        table_formats = ["csv", "parquet"]
        df.loc[
            # # filter desired file formats
            (df[PREFIX_RESOURCE_COLS + "format"].str.lower().isin(table_formats))
            &
            # do not filter geo data csvs
            (~df["tags"].apply(lambda tag: "geodaten" in tag)),
            "format_filter",
        ] = "table_data"

    if "geo_data" in desired_formats:
        df.loc[
            # # filter desired file formats
            (df[PREFIX_RESOURCE_COLS + "url"].str.contains("geojson"))
            &
            # only filter resources from the city of Zürich (not canton)
            (df["tags"].apply(lambda tag: "stzh" in tag)),
            "format_filter",
        ] = "geo_data"

    return df[df["format_filter"].notna()]


def extract_keywords(x, sep=","):
    """
    Extract keywords from ckan metadata json. To be used in pandas.apply()
    Example: [{'description': '', 'display_name': 'Mobilität'},]
    """
    out_string = ""
    for elem in x:
        out_string += elem["display_name"] + sep
    return out_string.rstrip(sep)


def clean_features(data):
    """Clean various features"""
    # Reduce publisher data to name.
    # In rare cases the publisher is not provided.
    data["publisher"] = data["author"]

    # Reduce tags to tag names.
    data.tags = data.tags.apply(lambda x: [tag["name"] for tag in x])

    # keywords/groups
    data["keywords"] = data["groups"].apply(extract_keywords)

    return data


def prepare_data_for_codebooks(data):
    """Prepare metadata from catalogue in order to create the code files"""
    # Add new features to save prepared data.
    print("Preparation for codebook files")

    data["metadata"] = None
    data["contact"] = ""
    data["distributions"] = None
    data["distribution_links"] = None

    # Iterate over datasets and create additional data for markdown and code cells.
    for idx in data.index:
        md = [f"- **{k.capitalize()}** `{data.loc[idx, k]}`\n" for k in KEYS_DATASET]
        data.loc[idx, "metadata"] = "".join(md)

    data["description"] = data["notes"]
    # Sort values for table.
    data.sort_values(by=["title", "name", PREFIX_RESOURCE_COLS + "name"], inplace=True)
    data.reset_index(drop=True, inplace=True)

    return data


def create_rmarkdown(data, notebook_template):
    """Create R Markdown files with R starter code"""
    print(
        "Creating", data.shape[0], "R Markdown files with template:", notebook_template
    )
    for idx in data.index:
        with open(
            os.path.join(TEMPLATE_FOLDER, notebook_template), "r", encoding="utf-8"
        ) as file:
            rmd = file.read()

        # Populate template with metadata.
        title = f"Open Government Data, {PROVIDER}"
        rmd = rmd.replace("{{ DOCUMENT_TITLE }}", title)

        title = re.sub('"', "'", data.loc[idx, f"title"])
        rmd = rmd.replace("{{ DATASET_TITLE }}", title)

        rmd = rmd.replace("{{ TODAY_DATE }}", TODAY_DATE)
        rmd = rmd.replace("{{ DATASET_IDENTIFIER }}", data.loc[idx, "name"])

        description = data.loc[idx, f"description"]
        description = re.sub('"', "'", description)
        description = re.sub("\\\\", "|", description)
        rmd = rmd.replace("{{ DATASET_DESCRIPTION }}", description)

        ssz_comments = str(data.loc[idx, "sszBemerkungen"])
        ssz_comments = re.sub('"', "'", ssz_comments)
        ssz_comments = re.sub("\\\\", "|", ssz_comments)
        rmd = rmd.replace("{{ SSZ_COMMENTS }}", ssz_comments)

        rmd = rmd.replace("{{ DATASET_METADATA }}", data.loc[idx, "metadata"])
        rmd = rmd.replace("{{ CONTACT }}", data.loc[idx, "maintainer_email"])

        url = f"[Direct link by **{PROVIDER}** for dataset]({BASELINK_DATAPORTAL}{data.loc[idx, 'name']})"
        rmd = rmd.replace("{{ DATASHOP_LINK_PROVIDER }}", url)

        # Get file URL and format
        file_url = data.loc[idx, PREFIX_RESOURCE_COLS + "url"]
        rmd = rmd.replace("{{ FILE_URL }}", file_url)

        # Save to disk.
        with open(
            f"{data.loc[idx, 'name']}_{data.loc[idx, PREFIX_RESOURCE_COLS + 'id']}.Rmd",
            "w",
            encoding="utf-8",
        ) as file:
            file.write("".join(rmd))


# %%
# CREATE CODE FILES ---------------------------------------------------------- #
def main(dataset_id):
    # Get the dataset
    odz = OpenDataZH()
    package = odz.get_package(dataset_id)
    df = dataset_to_resource(pd.DataFrame([package.metadata]))
    df = clean_features(df)
    df = prepare_data_for_codebooks(df)
    df = filter_resources(df)
    create_rmarkdown(df[df["format_filter"] == "table_data"], TEMPLATE_RMARKDOWN)
    create_rmarkdown(df[df["format_filter"] == "geo_data"], TEMPLATE_RMARKDOWN_GEO)


# %%
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Error: please provide a dataset id to generate R Markdown starter templates for."
        )
        sys.exit(1)
    main(sys.argv[1])
