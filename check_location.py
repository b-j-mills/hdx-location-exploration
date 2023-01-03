import logging
import re
from fiona import listlayers
from geopandas import read_file
from glob import glob
from numpy import number
from os import mkdir
from os.path import basename, dirname, join
from pandas import read_csv, read_excel
from shutil import rmtree
from zipfile import ZipFile, is_zipfile

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


def read_downloaded_data(resource_files, fileext):
    data = dict()
    error = None
    for resource_file in resource_files:
        if fileext in ["xlsx", "xls"]:
            contents = read_excel(
                resource_file, sheet_name=None, nrows=50
            )
            for key in contents:
                data[get_uuid()] = parse_excel(contents[key])
        if fileext == "csv":
            try:
                data = {
                    get_uuid(): read_csv(resource_file, nrows=50)
                }
            except:
                error = f"Unable to read resource {basename(resource_file)}"
                continue
        if fileext in ["geojson", "json", "shp", "topojson"]:
            try:
                data = {
                    get_uuid(): read_file(resource_file, rows=50)
                }
            except:
                error = f"Unable to read resource {basename(resource_file)}"
                continue
        if fileext in ["gdb", "gpkg"]:
            try:
                data = {
                    get_uuid(): read_file(dirname(resource_file), layer=basename(resource_file), rows=50)
                }
            except:
                error = f"Unable to read resource {basename(resource_file)}"
                continue

    return data, error


def parse_excel(df):
    df = df.dropna(how="all", axis=0).dropna(how="all", axis=1)
    df = df.fillna(method="ffill", axis=0).reset_index(drop=True)
    if not any([bool(re.match("Unnamed.*", c, re.IGNORECASE)) for c in df.columns]):
        return df
    headers = []
    i = 0
    while i < 10 and len(headers) == 0:
        headers = df.loc[i]
        if any(headers.isna()):
            headers = []
            i += 1
    if len(headers) > 0:
        df.columns = headers
        df = df.drop(range(i+1), axis=0).reset_index(drop=True)

    return df


def check_pcoded(contents):
    pcoded = None
    for key in contents:
        if pcoded:
            continue
        content = contents[key]
        content = content.select_dtypes(include=["string", "object"])
        pcodes = [h for h in content.columns if bool(re.match(".*p?.?cod.*", h, re.IGNORECASE))]
        if len(pcodes) == 0:
            continue
        for pcode in pcodes:
            if pcoded:
                continue
            column = content[pcode].dropna()
            matches = sum(column.str.match("[a-z]{2,3}\d{1,8}", case=False))
            if matches > (len(column) - 5) and matches > 0:
                pcoded = True

    return pcoded


def check_latlong(contents):
    latlonged = None
    for key in contents:
        if latlonged:
            continue
        content = contents[key]
        content = content.select_dtypes(include=number)
        lats = [h for h in content.columns if bool(re.match("(.*latitude?.*)|(lat)|((point.*)?y)", h, re.IGNORECASE))]
        lons = [h for h in content.columns if bool(re.match("(.*longitude?.*)|(lon(g)?)|((point.*)?x)", h, re.IGNORECASE))]
        if (len(lats) == 0) or (len(lons) == 0):
            continue
        content = content[lats + lons]
        latted = None
        longed = None
        for column in [lats + lons]:
            if latted and longed:
                continue
            column = content[column].dropna()
            if column in lats:
                matches = sum(column.between(-90, 90))
                if matches > (len(column) - 5) and matches > 0:
                    latted = True
            if column in lons:
                matches = sum(column.between(-180, 180))
                if matches > (len(column) - 5) and matches > 0:
                    longed = True
        if latted and longed:
            latlonged = True
    return latlonged


def check_location(dataset, temp_folder):
    pcoded = None
    latlonged = None
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
        if latlonged or not any(f in ["csv", "json", "xls", "xlsx"] for f in filetypes):
            continue

        logger.info(f"Checking {resource['name']}")

        filetype = resource.get_file_type()
        fileext = filetype
        if fileext == "geodatabase":
            fileext = "gdb"
        if fileext == "geopackage":
            fileext = "gpkg"

        if filetype not in allowed_filetypes:
            continue

        resource_files, error = download_resource(resource, fileext, resource_folder)
        if not resource_files:
            return pcoded, latlonged, error

        contents, error = read_downloaded_data(resource_files, fileext)

        if not pcoded:
            pcoded = check_pcoded(contents)
        if not pcoded and not latlonged and filetype in ["csv", "json", "xls", "xlsx"]:
            latlonged = check_latlong(contents)

    if not error and not pcoded:
        pcoded = False
    if not error and not pcoded and not latlonged:
        latlonged = False

    rmtree(resource_folder)

    return pcoded, latlonged, error
