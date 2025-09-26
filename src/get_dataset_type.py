# %%
import sys
from opendata import OpenDataZH


def main(dataset_id):
    # Get the dataset
    odz = OpenDataZH()
    package = odz.get_package(dataset_id)
    if len(package.geo_resource_metadata_df) > 0:
        print("geo")
        sys.exit(0)
    if len(package.tabular_resource_metadata_df) > 0:
        print("csv")
        sys.exit(0)
    print("unknown")
    sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: please provide a dataset id to inspect.")
        sys.exit(1)
    main(sys.argv[1])
