import logging
from os.path import join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from check_location import get_global_pcodes, process_resource

setup_logging()
logger = logging.getLogger(__name__)


def main(**ignore):

    configuration = Configuration.read()

    with temp_dir(folder="TempPCodeDetector") as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            retriever = Retrieve(
                downloader, temp_folder, "saved_data", temp_folder, save=False, use_saved=False
            )
            global_pcodes, global_miscodes = get_global_pcodes(
                configuration["global_pcodes"],
                retriever,
            )
            datasets = Dataset.get_all_datasets(rows=100)
            for dataset in datasets:
                resources = dataset.get_resources()
                for resource in resources:
                    pcoded, mis_pcoded = process_resource(
                        resource,
                        dataset,
                        global_pcodes,
                        global_miscodes,
                        retriever,
                        configuration,
                        update=False,
                    )
                    logger.info(f"{resource['name']}: {pcoded}, {mis_pcoded}")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="feature",
        user_agent="LocationExploration",
        hdx_read_only=False,
        preprefix="HDXINTERNAL",
        project_config_yaml=join("config", "project_configuration.yml"),
    )
