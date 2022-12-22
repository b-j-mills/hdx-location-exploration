import logging
import re
from fiona import listlayers
from geopandas import read_file
from glob import glob
from os import mkdir
from os.path import basename, dirname, join
from pandas import read_csv, read_excel
from shutil import rmtree
from zipfile import is_zipfile, ZipFile

from hdx.utilities.uuid import get_uuid

logger = logging.getLogger(__name__)


def download_resource(resource, fileext, resource_folder):
    try:
        _, resource_file = resource.download(folder=resource_folder)
    except:
        error = f"Could not download file {resource['name']}"
        return None, error

    if is_zipfile(resource_file) or ".zip" in basename(resource_file):
        temp = join(resource_folder, get_uuid())
        try:
            with ZipFile(resource_file, "r") as z:
                z.extractall(temp)
        except:
            error = f"Could not unzip resource {resource['name']}"
            return None, error
        resource_files = glob(join(temp, "**", f"*.{fileext}"), recursive=True)
        if len(resource_files) > 1:  # make sure to remove directories containing the actual files
            resource_files = [r for r in resource_files
                              if sum([r in rs for rs in resource_files if not rs == r]) == 0]
        if fileext == "xlsx" and len(resource_files) == 0:
            resource_files = [resource_file]
        if fileext in ["gdb", "gpkg"]:
            resource_files = [join(r, i) for r in resource_files for i in listlayers(r)]
    else:
        resource_files = [resource_file]

    return resource_files, None


def read_data(resource_files, fileext):
    data_to_check = dict()
    error = None
    for resource_file in resource_files:
        data = dict()
        if fileext in ["xlsx", "xls"]:
            data = read_excel(resource_file, sheet_name=None)
        if fileext == "csv":
            try:
                data = {basename(resource_file): read_csv(resource_file)}
            except:
                error = f"Unable to read resource {basename(resource_file)}"
                continue
        if fileext in ["geojson", "json", "shp", "topojson"]:
            try:
                data = {basename(resource_file): read_file(resource_file)}
            except:
                error = f"Unable to read resource {basename(resource_file)}"
                continue
        if fileext in ["gdb", "gpkg"]:
            try:
                data = {basename(resource_file): read_file(dirname(resource_file), layer=basename(resource_file))}
            except:
                error = f"Unable to read resource {basename(resource_file)}"
                continue

        for key in data:
            data_to_check[key] = data[key]

    return data_to_check, error


def check_location(dataset, downloader, temp_folder):
    pcoded = None
    latlong = None
    error = None

    allowed_filetypes = ["csv", "geodatabase", "geojson", "geopackage", "json",
                         "shp", "topojson", "xls", "xlsx"]
    filetypes = dataset.get_filetypes()
    if not any(f in allowed_filetypes for f in filetypes):
        error = "Can't check formats"
        return None, None, error

    resource_folder = join(temp_folder, get_uuid())
    mkdir(resource_folder)

    resources = dataset.get_resources()
    for resource in resources:
        if pcoded:
            continue

        filetype = resource.get_file_type()
        fileext = filetype
        if fileext == "geodatabase":
            fileext = "gdb"
        if fileext == "geopackage":
            fileext = "gpkg"

        if filetype not in allowed_filetypes:
            continue

        headers = dict()
        if fileext in ["csv", "xls", "xlsx"] and ".zip" not in basename(resource["url"]):
            # these we can read directly off HDX, no need to download
            header, _ = downloader.get_tabular_rows(resource["url"], dict_form=True)
            headers[resource["name"]] = header.to_list()

        else:
            resource_files, error = download_resource(resource, fileext, resource_folder)
            if not resource_files:
            return pcoded, latlong, error

            data_to_check, error = read_downloaded_data(resource_files, fileext)
            if len(data_to_check) == 0:
            return pcoded, latlong, error

            for data in data_to_check:
                headers[data] = data_to_check[data].columns

        pcodes = [bool(re.match(".*p.?cod.*", h, re.IGNORECASE)) for header in headers for h in headers[header]]
        if any(pcodes):
            pcoded = True
            continue

    if not error and not pcoded:
        pcoded = False

    rmtree(resource_folder)

    return pcoded, latlong, error
