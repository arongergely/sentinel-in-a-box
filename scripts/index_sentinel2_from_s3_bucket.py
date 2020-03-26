###########################################################
# ODC prepare script for indexing Sentinel-2 from S3 Bucket
###########################################################
# Author:   Aron Gergely
# Email:    aron.gergely@rasterra.nl
# Credits:  ODC Contributors, Ideas based on scripts by
#           Damien Ayers
###########################################################
import logging
import xml.etree.ElementTree as ET
import uuid
import re
from multiprocessing import Process, Manager, current_process, cpu_count
from queue import Empty

import boto3
from osgeo import osr
import click

import datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes

GUARDIAN = "GUARDIAN QUEUE EMPTY"

#BUCKET_NAME = 'sentinel-s2-l2a'
#PREFIX = 'tiles/16/R/BV/2019/10/10'

SUFFIX_TILEINFO = 'tileInfo.json'
SUFFIX_PRODUCTINFO = 'productInfo.json'
SUFFIX_METADATA = 'metadata.xml'

sentinels = {'S2A': 'SENTINEL_2A',
             'S2B': 'SENTINEL_2B'}

# {band name: s3 key suffix}
bands_10m = {'B02_10m': 'R10m/B02.jp2',
             'B03_10m': 'R10m/B03.jp2',
             'B04_10m': 'R10m/B04.jp2',
             'B08_10m': 'R10m/B08.jp2'}

bands_20m = {'B05_20m': 'R20m/B05.jp2',
             'B06_20m': 'R20m/B06.jp2',
             'B07_20m': 'R20m/B07.jp2',
             'B8A_20m': 'R20m/B8a.jp2',
             'B11_20m': 'R20m/B11.jp2',
             'B12_20m': 'R20m/B12.jp2',
             'SCL_20m': 'R20m/SCL.jp2'}

bands_60m = {'B01_60m': 'R60m/B01.jp2',
             'B09_60m': 'R60m/B09.jp2',
             'B10_60m': 'R60m/B09.jp2'}

bands_resampled = {'B02_20m': 'R20m/B02.jp2',
                   'B03_20m': 'R20m/B03.jp2',
                   'B04_20m': 'R20m/B04.jp2',
                   'B08_20m': 'R20m/B08.jp2',
                   'B08_60m': 'R60m/B08.jp2',
                   'B8A_60m': 'R60m/B8A.jp2',
                   'B03_60m': 'R60m/B03.jp2',
                   'B06_60m': 'R60m/B06.jp2',
                   'B02_60m': 'R60m/B02.jp2',
                   'B05_60m': 'R60m/B05.jp2',
                   'B04_60m': 'R60m/B04.jp2',
                   'B07_60m': 'R60m/B07.jp2',
                   'B11_60m': 'R60m/B11.jp2',
                   'B12_60m': 'R60m/B12.jp2'}

bands = {**bands_10m, **bands_20m, **bands_60m, **bands_resampled}


def format_s3_url(*args):
    return '/'.join(('s3:/', *args))


def archive_document(doc, uri, index, sources_policy):
    def get_ids(dataset):
        ds = index.datasets.get(dataset.id, include_sources=True)
        for source in ds.sources.values():
            yield source.id
        yield dataset.id

    resolver = Doc2Dataset(index)
    dataset, err = resolver(doc, uri)
    index.datasets.archive(get_ids(dataset))
    logging.info("Archiving %s and all sources of %s", dataset.id, dataset.id)


def add_dataset(doc, uri, index, sources_policy):
    logging.info("Indexing %s", uri)
    resolver = Doc2Dataset(index)
    dataset, err = resolver(doc, uri)
    if err is not None:
        logging.error("%s", err)
    else:
        try:
            index.datasets.add(dataset,
                               sources_policy=sources_policy)  # Source policy to be checked in sentinel 2 dataset types
        except changes.DocumentMismatchError as e:
            index.datasets.update(dataset, {tuple(): changes.allow_any})
        except Exception as e:
            err = e
            logging.error("Unhandled exception %s", e)

    return dataset, err


def transform_to_geodetic(geo_ref_points, epsg_string):

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(int(epsg_string.rpartition(':')[2]))
    t = osr.CoordinateTransformation(srs, srs.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}


def calculate_corner_coords(dictionary):

    x_ul = dictionary['ul_x']
    y_ul = dictionary['ul_y']
    rows = dictionary['nrows']
    cols = dictionary['ncols']
    x_dim = dictionary['xdim']
    y_dim = dictionary['ydim']

    corner_coords = {'ul': {'x': x_ul,
                            'y': y_ul},
                     'ur': {'x': x_ul + (rows * x_dim),
                            'y': y_ul},
                     'll': {'x': x_ul,
                            'y': y_ul + (cols * y_dim)},
                     'lr': {'x': x_ul + (rows * x_dim),
                            'y': y_ul + (cols * y_dim)}}
    return corner_coords


def bandpaths(bucket_name, s3_key_root):
    result = {}

    for name, key_suffix in bands.items():
        result[name] = {'path': '/'.join(('s3:/', bucket_name, s3_key_root, key_suffix)),
                        'layer': 1}
    return result


def process_xml(xml_bytes):

    namespaces = {'n1': 'https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-2A_Tile_Metadata.xsd'}
    tags = {'sensing_time': 'n1:General_Info/SENSING_TIME',
            'tile_id': 'n1:General_Info/TILE_ID',
            'epsg': 'n1:Geometric_Info/Tile_Geocoding/HORIZONTAL_CS_CODE',
            'nrows': 'n1:Geometric_Info/Tile_Geocoding/Size[@resolution=\'10\']/NROWS',
            'ncols': 'n1:Geometric_Info/Tile_Geocoding/Size[@resolution=\'10\']/NCOLS',
            'ul_x': 'n1:Geometric_Info/Tile_Geocoding/Geoposition/[@resolution=\'10\']/ULX',
            'ul_y': 'n1:Geometric_Info/Tile_Geocoding/Geoposition/[@resolution=\'10\']/ULY',
            'xdim': 'n1:Geometric_Info/Tile_Geocoding/Geoposition/[@resolution=\'10\']/XDIM',
            'ydim': 'n1:Geometric_Info/Tile_Geocoding/Geoposition/[@resolution=\'10\']/YDIM',
            'cloudcover': 'n1:Quality_Indicators_Info/Image_Content_QI/CLOUDY_PIXEL_PERCENTAGE'}

    et = ET.fromstring(xml_bytes)

    tp = {}
    for tag, string in tags.items():
        value = et.find(string, namespaces).text
        # get the satellite platform and tile details from tile id
        if tag == 'tile_id':
            pl_match = re.search(r'^(S2[AB])_', value)
            if pl_match.group(1) in ('S2A', 'S2B'):
                tp['platform'] = sentinels[pl_match.group(1)]
            ti_match = re.search(r'_T([0-9]{2})([A-Z])([A-Z]{2})_', value)
            if ti_match:
                for k, v in zip(('zone', 'latband', 'square'),  ti_match.groups()):
                    tp[k] = v

        if tag in ('ncols', 'nrows', 'ul_x', 'ul_y', 'xdim', 'ydim'):
            tp[tag] = int(value)
            continue
        elif tag == 'cloudcover':
            tp[tag] = float(value)
            continue
        tp[tag] = value.strip()
    print(tp)
    return tp


def assemble_metadata_doc(metadata_dict, bucket_name, object_key):
    print('assemble_metadata_doc()')

    epsg_string = metadata_dict['epsg']
    epsg_code = epsg_string.rpartition(':')[2]
    coord_corners = calculate_corner_coords(metadata_dict)
    key_root = object_key.rpartition('/')[0]
    doc = {'id': str(uuid.uuid5(uuid.NAMESPACE_URL, format_s3_url(bucket_name, object_key))),
           'processing_level': 'Level-2A',
           'product_type': 'S2MSI2A',
           'creation_dt': '',
           'label': metadata_dict['tile_id'],
           'platform': {'code': metadata_dict['platform']},
           'instrument': {'name': 'MSI'},
           'extent': {'from_dt': metadata_dict['sensing_time'],
                      'to_dt': metadata_dict['sensing_time'],
                      'center_dt': metadata_dict['sensing_time'],
                      'coord': transform_to_geodetic(coord_corners, epsg_code)},
           'format': {'name': 'JPEG2000'},
           'grid_spatial': {'projection': {'geo_ref_points': coord_corners,
                                           'spatial_reference': epsg_string}},
           'image': {'bands': bandpaths(bucket_name, key_root)},
           'cloud_cover': metadata_dict['cloudcover'],
           'utm_zone': metadata_dict['zone'],
           'latitude_band': metadata_dict['latband'],
           'square': metadata_dict['square'],
           'lineage': {'source_datasets': {}}}

    return doc

def worker(bucket_name, queue, func, sources_policy):
    #print("worker()")

    dc = datacube.Datacube()
    index = dc.index
    session = boto3.session.Session()
    s3 = session.resource('s3')

    while True:
        try:
            key = queue.get(timeout=60)
            if key == GUARDIAN:
                break

            logging.info("Processing %s %s", key, current_process())
            s3_obj = s3.Object(bucket_name, key).get(ResponseCacheControl='no-cache', RequestPayer='requester')
            xml_bytes = s3_obj['Body'].read()
            metadata_processed = process_xml(xml_bytes)
            data = assemble_metadata_doc(metadata_processed, bucket_name, key)

            uri = format_s3_url(bucket_name, key)
            logging.info("calling %s", add_dataset)
            func(data, uri, index, sources_policy)
            queue.task_done()
        except Empty:
            break
        except EOFError:
            break


def iterate_datasets(bucket_name, prefix, func, sources_policy, config=None, suffix=None):
    manager = Manager()
    queue = manager.Queue()

    session = boto3.session.Session()
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    logging.info("Bucket : %s prefix: %s ", bucket_name, str(prefix))
    worker_count = cpu_count() * 2

    processes = []
    for i in range(worker_count):
        proc =Process(target=worker, args=(bucket_name, queue, func, sources_policy))
        processes.append(proc)
        proc.start()

    for obj in bucket.objects.filter(Prefix=prefix, RequestPayer='requester'):
        if obj.key.endswith(SUFFIX_METADATA):
            queue.put(obj.key)

    for i in range(worker_count):
        queue.put(GUARDIAN)

    for proc in processes:
        proc.join()

    print("Finished")

@click.command(help='Enter Bucket name. Optional to enter configuration file to access a different database')
@click.argument('bucket_name')
@click.option('--prefix', '-p', help="Pass the prefix of the object to the bucket")
@click.option('--archive', is_flag=True,
              help="If true, datasets found in the specified bucket and prefix will be archived")
@click.option('--sources_policy', default="verify", help="verify, ensure, skip")
def main(bucket_name, prefix, archive, sources_policy):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    action = archive_document if archive else add_dataset
    iterate_datasets(bucket_name, prefix, action, sources_policy)


if __name__ == "__main__":
    main()
