import csv
import logging
import warnings
from shapely.errors import ShapelyDeprecationWarning

from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import temp_dir

from check_location import check_location, get_global_pcodes

setup_logging()
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


def main(**ignore):
    allowed_filetypes = ["csv", "geodatabase", "geojson", "geopackage", "json",
                         "shp", "topojson", "xls", "xlsx"]

    with temp_dir(folder="TempLocationExploration") as temp_folder:
        datasets = Dataset.search_in_hdx(
            fq='cod_level:"cod-standard"'
        )
        logger.info(f"Found {len(datasets)} datasets")

        global_pcodes = get_global_pcodes()

        with open("datasets_location_status.csv", "w") as c:
            writer = csv.writer(c)
            writer.writerow([
                "dataset name",
                "resource name",
                "pcoded",
                "latlonged",
                "error",
            ])

            for dataset in datasets:
                logger.info(f"Checking {dataset['name']}")

                resources = dataset.get_resources()
                for resource in resources:
                    if resource.get_file_type() not in allowed_filetypes:
                        writer.writerow([
                            dataset["name"],
                            resource["name"],
                            None,
                            None,
                            f"{resource.get_file_type()} can't be p-coded",
                        ])
                        continue

                    pcoded, latlonged, error = check_location(resource, global_pcodes, temp_folder)
                    writer.writerow([
                        dataset["name"],
                        resource["name"],
                        pcoded,
                        latlonged,
                        error,
                    ])


if __name__ == "__main__":
    facade(
        main,
        hdx_site="prod",
        user_agent="LocationExploration",
        hdx_read_only=True,
        preprefix="HDXINTERNAL",
    )
