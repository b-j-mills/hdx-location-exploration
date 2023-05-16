import logging
from os.path import join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import temp_dir

from check_location import check_location, get_global_pcodes

setup_logging()
logger = logging.getLogger(__name__)


def main(**ignore):

    configuration = Configuration.read()

    with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
        global_pcodes, global_miscodes = get_global_pcodes(
            configuration["global_pcodes"],
            downloader,
        )

    with temp_dir(folder="TempLocationExploration") as temp_folder:
        datasets = Dataset.search_in_hdx(fq='groups:"tur"')
        logger.info(f"Found {len(datasets)} datasets")

        for dataset in datasets:
            # logger.info(f"Checking {dataset['name']}")

            locations = dataset.get_location_iso3s()
            pcodes = [pcode for iso in global_pcodes for pcode in global_pcodes[iso] if iso in locations]
            miscodes = [pcode for iso in global_miscodes for pcode in global_miscodes[iso] if iso in locations]

            resources = dataset.get_resources()
            for resource in resources:
                if dataset.get_organization()["name"] == "hot":
                    resource["p_coded"] = False
                    continue

                if resource.get_file_type() not in configuration["allowed_filetypes"]:
                    resource["p_coded"] = False
                    continue

                if resource["size"] and resource["size"] > configuration["resource_size"]:
                    resource["p_coded"] = False
                    continue

                pcoded, mis_pcoded, error = check_location(resource, pcodes, miscodes, temp_folder)
                if mis_pcoded:
                    logger.warning(f"{dataset['name']}: {resource['name']}: may be mis-pcoded")

                if pcoded is None:
                    logger.error(f"{dataset['name']}: {resource['name']}: {error}")
                    continue

                resource["p_coded"] = pcoded

            try:
                dataset.update_in_hdx(
                    hxl_update=False,
                    operation="patch",
                    batch_mode="KEEP_OLD",
                    skip_validation=True,
                    ignore_check=True,
                )
            except HDXError:
                logger.exception(f"Could not update {dataset['name']}")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="feature",
        user_agent="LocationExploration",
        hdx_read_only=False,
        preprefix="HDXINTERNAL",
        project_config_yaml=join("config", "project_configuration.yml"),
    )
