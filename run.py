import logging
import warnings
from shapely.errors import ShapelyDeprecationWarning

from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.path import temp_dir

from check_location import check_location, get_global_pcodes

setup_logging()
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


def main(**ignore):
    allowed_filetypes = ["csv", "geodatabase", "geojson", "geopackage", "json",
                         "shp", "topojson", "xls", "xlsx"]

    with temp_dir(folder="TempLocationExploration") as temp_folder:
        datasets = Dataset.search_in_hdx(fq='groups:"ben"')
        logger.info(f"Found {len(datasets)} datasets")

        global_pcodes, global_miscodes = get_global_pcodes(
            "https://raw.githubusercontent.com/b-j-mills/hdx-global-pcodes/main/global_pcodes.csv"
        )

        status = [["dataset name", "resource name", "format", "pcoded", "mis_pcoded", "error"]]

        for dataset in datasets:
            logger.info(f"Checking {dataset['name']}")

            resources = dataset.get_resources()
            for resource in resources:
                if resource.get_file_type() not in allowed_filetypes:
                    status.append([
                        dataset["name"],
                        resource["name"],
                        resource.get_file_type(),
                        None,
                        None,
                        "Not checking format",
                    ])
                    continue

                if resource["size"] and resource["size"] > 1073741824:
                    status.append([
                        dataset["name"],
                        resource["name"],
                        resource.get_file_type(),
                        None,
                        None,
                        "Not checking files of this size",
                    ])
                    continue

                pcoded, mis_pcoded, error = check_location(resource, global_pcodes, global_miscodes, temp_folder)
                status.append([
                    dataset["name"],
                    resource["name"],
                    resource.get_file_type(),
                    pcoded,
                    mis_pcoded,
                    error,
                ])

        write_list_to_csv("datasets_location_status.csv", status)


if __name__ == "__main__":
    facade(
        main,
        hdx_site="feature",
        user_agent="LocationExploration",
        hdx_read_only=True,
        preprefix="HDXINTERNAL",
    )
