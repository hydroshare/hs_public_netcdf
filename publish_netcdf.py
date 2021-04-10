import os
import dotenv
import logging
import time
import textwrap
import argparse
import stat
import glob
import pathlib
import datetime
import shutil
import subprocess
from irods.session import iRODSSession
from irods.meta import iRODSMeta

RESOURCE_ID_GLOB = "????????????????????????????????"
EXCLUDED = ["bags", "temp", "zips"]
IS_PUBLIC_KEY = "isPublic"
IS_PUBLIC_VALUE = "true"
NETCDF_EXTENSION = ".nc"
FILE_MODE = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

logger = logging.getLogger(__name__)


class NetCDFPublicationError(Exception):
    """
    An Exception class for NetCDF publication.
    """

    pass


def rchmod(path, mode):
    """
    Recursively change filesystem permissions of path and all of its children.'

    rchmod(path, mode) -> None

    Where:
        path: <str> Absolute path to change filesystems permissions
        mode: <int> numeric mode for all changes consistent with constants in the stats library
    """

    os.chmod(path, mode)
    for root, dirs, files in os.walk(path):
        for d in dirs:
            os.chmod(os.path.join(root, d), mode)
        for f in files:
            os.chmod(os.path.join(root, f), mode)
    return None


def replace_spaces_in_names(path):
    """
    Recursively replace spaces in names of all of the children underneath a path.'

    replace_spaces_in_names(path) -> None

    Where:
        path: <str> Absolute path to traverse with name fixes

    This is a fix for a bug in TDS 5 which was already fixed in TDS 4 but has regressed.
    When a fix is available in TDS 5 and then deployed, this function may be deprecated.

    Spaces are replaced with dunders as cases have been encountered where replacing
    with a single underscore resulted in a name collision.
    """

    replaced = 0
    walk = list(os.walk(path))
    walk.reverse()
    for root, dirs, files in walk:
        for f in files:
            if " " in f:
                os.rename(os.path.join(root, f), os.path.join(root, f.replace(" ", "__")))
                replaced += 1
        for d in dirs:
            if " " in d:
                os.rename(os.path.join(root, d), os.path.join(root, d.replace(" ", "__")))
                replaced += 1
    if replaced:
        logger.warning(f"Replaced {replaced} name{'s' if replaced != 1 else ''} " \
                       f"of {'a ' if replaced == 1 else ''}child{'ren' if replaced != 1 else ''} " \
                       f"in destination path {path}")
    return None


def get_latest_resource_timestamp(irods_env, collection_path):
    """
    Return the latest modifcation time among the collection's data objects.

    get_latest_resource_timestamp(collection_path) -> <datetime.datetime>

    Where:
        irods_env:    <str> Absolute path to the iRODS environment file
        collection_path: <str> Absolute iRODS path to the collection

    Returns: <datetime.datetime> The latest modification time

    This function should become deprecated with iRODS 4.2.9 which updates collection modification times
    whenever a contained data object is modified.
    """

    with iRODSSession(irods_env_file=irods_env) as session:
        collection = session.collections.get(collection_path)
        tree = [leaf for leaf in collection.walk()]
        data_objects = []
        for leaf in tree:
            data_objects.extend(leaf[2])
        timestamps = [data_object.modify_time for data_object in data_objects]

    timestamp = max(timestamps)
    return timestamp


def publish_resource(irods_env, proxy_path, catalog_path, resource_id):
    """
    Copy the resource with its timestamp.

    publish_resource(proxy_path, catalog_path, resource_id) -> None

    Where:
        irods_env:    <str> Absolute path to the iRODS environment file
        proxy_path:   <str> Absolute iRODS proxy path to Hydroshare resources
        catalog_path: <str> Absolute THREDDS catalog path to publish resources
        resource_id:  <str> Resource ID to publish

    Raises:
        NetCDFPublicationError
    """

    logger.info(f"Publishing resource ID: {resource_id} from {proxy_path} to {catalog_path}")
    source = os.path.join(proxy_path, resource_id)
    destination = os.path.join(catalog_path, resource_id)

    timestamp = get_latest_resource_timestamp(irods_env, source)

    # The iget destination is the catalog path in light of https://github.com/irods/irods/issues/5527
    proc = subprocess.Popen(["env", f"IRODS_ENVIRONMENT_FILE={irods_env}", "iget", "-rf", source, catalog_path],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode:
        logger.error(f"Publishing resource ID: {resource_id} from {proxy_path} to {catalog_path} failed:" \
                     f"return code: {proc.returncode} ::: " \
                     f'stdout: {stdout} ::: ' \
                     f"stderr: {stderr}")
        raise NetCDFPublicationError(f"iget {source} to {destination} failed",
                                     proc.returncode,
                                     stdout,
                                     stderr)
    rchmod(destination, FILE_MODE)
    # Fix for TDS 5. Hope to see a fix for this in TDS 5 itself.
    replace_spaces_in_names(destination)
    os.utime(destination, (timestamp.timestamp(), timestamp.timestamp()))
    logger.info(f"Published resource ID: {resource_id} from {proxy_path} to {catalog_path} with timestamp: {timestamp}")
    return None


def scan_source(irods_env, proxy_path):
    """
    Scan the iRODS proxy path for all public Hydroshare resources containing NetCDF and their timestamps.

    scan_source(irods_env, proxy_path) -> [(resource_id, timestamp), ...]

    Where:
        irods_env:    <str> Absolute path to the iRODS environment file
        proxy_path:   <str> Absolute iRODS proxy path to Hydroshare resources

    Returns: <list> of two-<tuple>s where:
        a) first element is a <str> resource id, and
        b) second element is a <datetime.datetime> modification time.
    """

    with iRODSSession(irods_env_file=irods_env) as session:
        subcollections = session.collections.get(proxy_path).subcollections
        subcollections = [subcollection for subcollection in subcollections if subcollection.name not in EXCLUDED]
        logger.info(f"Number of included subcollections: {len(subcollections)}")

        public = [subcollection for subcollection in subcollections
                  if "isPublic" in subcollection.metadata.keys()
                  and subcollection.metadata[IS_PUBLIC_KEY].value == IS_PUBLIC_VALUE]
        logger.info(f"Number of public included subcollections: {len(public)}")

        public_netcdf = []
        for subcollection in public:
            public_objects = [objs for col, subcol, objs in list(subcollection.walk())]
            # flatten the list of lists of data objects
            data_objects = []
            for objs in public_objects:
                data_objects.extend(objs)
            netcdf_objects = [obj for obj in data_objects if obj.name.lower().endswith(NETCDF_EXTENSION)]
            if netcdf_objects:
                public_netcdf.append(subcollection.name)
                logger.info(f"Subcollection name: {subcollection.name}; Number of NetCDF data objects in subcollection: {len(netcdf_objects)}")
        logger.info(f"Number of public subcollections containing NetCDF: {len(public_netcdf)}")

    source_netcdf = [(resource_id, get_latest_resource_timestamp(irods_env, os.path.join(proxy_path, resource_id)))
                     for resource_id in public_netcdf]
    return source_netcdf


def scan_destination(catalog_path):
    """
   Scan the THREDDS catalog path for all resources and their timestamps.

    scan_destination(catalog_path) -> [(resource_id, timestamp), ...]

    Where:
        catalog_path: <str> Absolute THREDDS catalog path to publish resources

    Returns: <list> of two-<tuple>s where:
        a) first element is a <str> resource id, and
        b) second element is a <datetime.datetime> modification time.
    """

    resources = glob.glob(os.path.join(catalog_path, RESOURCE_ID_GLOB))
    logger.info(f"Number of destination resources: {len(resources)}")
    destination_netcdf = [(pathlib.PurePath(resource).name, datetime.datetime.fromtimestamp(os.path.getmtime(resource)))
                          for resource in resources]    
    return destination_netcdf


def remove_resource(catalog_path, resource_id):
    """
    Remove a resource from the published destination.

    remove_resource(catalog_path, resource_id) -> None

    Where:
        catalog_path: <str> Absolute THREDDS catalog path to publish resources
        resource_id:  <str> The resource ID to remove from publication
    """
    
    shutil.rmtree(os.path.join(catalog_path, resource_id))
    logger.info(f"Removed resource ID: {resource_id}")
    return None


def sync_resources(irods_env, proxy_path, catalog_path):
    """
    Sync public netcdf resources between iRODS proxy and THREDDS catalog.

    sync_resource(irods_env, proxy_path, catalog_path) -> None

    Where:
        irods_env:    <str> Absolute path to the iRODS environment file
        proxy_path:   <str> Absolute iRODS proxy path to Hydroshare resources
        catalog_path: <str> Absolute THREDDS catalog path to publish resources

    a) Scan all resources in the source path and publish the public resources containing NetCDF which:
        i) do not exist in the destination path, or
        ii) are out of date in the destination path, and
    b) Scan all resources in the destination path and remove the resources which:
        i) no longer exist in the source path, or
        ii) are no longer public in the source path.
    """

    logger.info(f"Syncing resources from {proxy_path} to {catalog_path}")
    start_time = time.perf_counter()
    source_netcdf = scan_source(irods_env, proxy_path)
    destination_netcdf = scan_destination(catalog_path)
    destination_ids = [destination[0] for destination in destination_netcdf]
    destination_timestamps = [destination[1] for destination in destination_netcdf]
    for source_id, source_timestamp in source_netcdf:
        try:
            if source_id not in destination_ids:
                logger.info(f"Resource ID: {source_id} not in destination")
                publish_resource(irods_env, proxy_path, catalog_path, source_id)
            else:
                index = destination_ids.index(source_id)
                destination_timestamp = destination_timestamps[index]
                if source_timestamp > destination_timestamp:
                    logger.info(f"Resource ID: {source_id} source timestamp: {source_timestamp} > destination timestamp: {destination_timestamp}")
                    publish_resource(irods_env, proxy_path, catalog_path, source_id)
        except NetCDFPublicationError as e:
            logger.warning(f"Syncing resources from {proxy_path} to {catalog_path} incomplete")
    destination_netcdf = scan_destination(catalog_path)
    source_ids = [source[0] for source in source_netcdf]
    for destination_id, destination_timestamp in destination_netcdf:
        if destination_id not in source_ids:
            logger.info(f"Resource ID: {destination_id} no longer in source")
            remove_resource(catalog_path, destination_id)
    end_time = time.perf_counter()
    run_time = end_time - start_time
    logger.info(f"Resources synced from {proxy_path} to {catalog_path} in {run_time:0.4f} seconds")
    return None


if __name__ == "__main__":
    epilog = """\
             If invoked with a resource ID argument, publish the resource to the destination path, assumed to be referenced in a THREDDS catalog.
             Otherwise,
                 a) scan all resources in the source path and publish the public resources containing NetCDF which:
                     i) do not exist in the destination path, or
                     ii) are out of date in the destination path, and
                 b) scan all resources in the destination path and remove the resources which:
                     i) no longer exist in the source path, or
                     ii) are no longer public in the source path."""
    parser = argparse.ArgumentParser(description="Publish public Hydroshare resources containing NetCDF.",
                                     epilog=textwrap.dedent(epilog),
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("dotenv_path",
                        help="Absolute path to the .env file.")
    parser.add_argument("resource_id",
                        nargs="?",
                        default="",
                        help=textwrap.dedent("""\
                                             Optional resource ID to publish.
                                             If not specified, publish all public Hydroshare resources containing NetCDF."""))
    args = parser.parse_args()

    dotenv.load_dotenv(dotenv.find_dotenv(args.dotenv_path))
    log_file = os.environ["PUBLIC_NETCDF_LOG_FILE"]
    irods_env = os.environ["PUBLIC_NETCDF_IRODS_ENVIRONMENT_FILE"]
    proxy_path = os.environ["PUBLIC_NETCDF_IRODS_PROXY_PATH"]
    catalog_path = os.environ['PUBLIC_NETCDF_THREDDS_CATALOG_PATH']

    logging.basicConfig(filename=log_file,
                        # Available in Python 3.9+
                        # encoding="utf-8",
                        level=logging.INFO,
                        format="[%(asctime)s] [%(levelname)s] %(message)s",
                        datefmt="%m/%d/%Y %I:%M:%S %p")
    logger = logging.getLogger(__name__)

    if args.resource_id:
        try:
            publish_resource(irods_env,
                             proxy_path,
                             catalog_path,
                             args.resource_id)
        except NetCDFPublicationError as e:
            logger.warning(f"Publishing resource {args.resource_id} from {args.src_path} to {args.dest_path} incomplete")
    else:
        sync_resources(irods_env,
                       proxy_path,
                       catalog_path)
