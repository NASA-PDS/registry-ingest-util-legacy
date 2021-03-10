import pystache
import argparse
import os
import logging
from tqdm import tqdm
from multiprocessing import Pool
import shutil
import re
import configparser

config = configparser.ConfigParser()
config.read(['pds_registry_ingest.ini.default',
    'pds_registry_ingest.ini'])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HARVEST_TEMPLATE = config.get('env', 'HARVEST_TEMPLATE')
CONF_DIR = config.get('env', 'CONF_DIR')
HARVEST_OUT_DIR = config.get('env', 'HARVEST_OUT_DIR') 
JAVA_HOME = config.get('env', 'JAVA_HOME')
HARVEST_DIR = config.get('env', 'HARVEST_DIR') 
REGISTRY_MGR_DIR = config.get('env', 'REGISTRY_MGR_DIR')
ELASTIC_SEARCH_URL =  config.get('elastic', 'url')
ELASTIC_AUTH_FILE =  config.get('elastic', 'auth-file')


def create_harvest_conf(root):
    renderer = pystache.Renderer()
    os.makedirs(CONF_DIR, exist_ok=True)
    harvest_conf_file = root.replace('/', '_') + '.xml' 
    harvest_conf_path = os.path.join(CONF_DIR, harvest_conf_file) 
    named_excluded_subdirs = [{'name': d} for d in os.listdir(root) if os.path.isdir(d)]
    with open(harvest_conf_path, 'w') as f:
        harvest_conf = renderer.render_path(HARVEST_TEMPLATE, root=root, subdirs=named_excluded_subdirs)
        logger.debug(f'harvest conf content is {harvest_conf}')
        f.write(harvest_conf)
    return harvest_conf_path


def harvest(harvest_conf_file):
    harvest_bin = os.path.join(HARVEST_DIR, 'bin', 'harvest')
    os.makedirs(HARVEST_OUT_DIR, exist_ok=True)
    harvest_output = os.path.join(HARVEST_OUT_DIR, os.path.basename(harvest_conf_file)[:-4])
    harvest_cmd = [harvest_bin, '-c', harvest_conf_file, '-o', harvest_output, '>>tmp/harvest.log']
    harvest_cmd_str = ' '.join(harvest_cmd)
    logger.debug(f'harvest with command {harvest_cmd_str}')
    os.system(harvest_cmd_str)
    return harvest_output


def load_data_to_registry(harvest_result):
    registry_bin = os.path.join(REGISTRY_MGR_DIR, 'bin', 'registry-manager')
    registry_cmd = [registry_bin, 'load-data', '-dir', harvest_result, '-es', ELASTIC_SEARCH_URL, '>>tmp/registry.log', ]
    if ELASTIC_AUTH_FILE:
        registry_cmd.extend(['-auth', ELASTIC_AUTH_FILE])
    registry_cmd_str = ' '.join(registry_cmd)
    logger.debug('load data to registry with command {registry_cmd_str}')
    os.system(registry_cmd_str)
    logger.debug(f'data {harvest_result} to loaded registry')
    shutil.rmtree(harvest_result)



def ingest(root):
    logger.debug(f'ingesting subdirectory {root}')
    harvest_conf_file = create_harvest_conf(root)
    harvest_result = harvest(harvest_conf_file)
    load_data_to_registry(harvest_result) 


def contains_bundle_xml(files):
    prog = re.compile(r'.*bundle.*.xml')
    for file in files:
        if prog.match(file):
            return True
    return False


def main():
    os.environ['JAVA_HOME'] = JAVA_HOME
    os.environ['HARVEST_HOME'] = HARVEST_DIR
    os.environ['REGISTRY_MGR_DIR'] = REGISTRY_MGR_DIR


    parser = argparse.ArgumentParser(description='ingest subdirectories in registry by pieces')
    parser.add_argument('root', metavar='dir', type=str, 
                    help='root directory where resources to ingest are')

    args = parser.parse_args()
    logger.info(f'scaniing directory {args.root}')
    n_dir = 0
    dirs_to_ingest = []
    for root, _, files in os.walk(args.root, topdown=False):
        logger.debug(f'process dir {root}')
        if contains_bundle_xml(files):
            print(root)
            dirs_to_ingest.append(root)
        n_dir+=1
        #if n_dir>3:
        #    break
    logger.info(f"launch processes for {len(dirs_to_ingest)} directories")
    with Pool(7) as p:
        p.map(ingest, dirs_to_ingest)
    


if __name__ == '__main__':
    main()
