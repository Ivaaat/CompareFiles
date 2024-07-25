import configparser
import sys
import argparse
from typing import Union

parsers = argparse.ArgumentParser(description='Text file comparison tool',)
parsers.prog = 'compare.py'
parsers.add_argument('-c', '--config', dest = 'config_file', default='config_comparison.ini')
args = parsers.parse_args()

config = configparser.ConfigParser()
config.read(args.config_file)

def split_param(param: Union[list, None], type_in=int) -> list:
        if param:
            return list(map(type_in, param.split(',')))
        else:
            return []

try:
    #general
    TYPE_DELIMITER = config['general']['type_delimiter']
    PREPARE_ETL = config.getboolean('general', 'prepare_etalon')
    PREPARE_SRC = config.getboolean('general', 'prepare_source')
    REGEX_RENAME = config['general']['regex_rename_files'] 
    MASK_FILES_ETALON = config['general']['mask_files_etalon']
    MASK_FILES_SOURCE = config['general']['mask_files_source'] 
    ENCODE_FILES_ETALON = config['general']['encode_files_etalon']
    ENCODE_FILES_SOURCE = config['general']['encode_files_source'] 
    LOG_IN_FILE = config.getboolean('general', 'log_in_file')
    LOG_FILE_MAX_SIZE =  config.getint('general', 'log_file_max_size')
    LOG_FILE_BACKUP_COUNT = config.getint('general', 'log_file_backup_count')
    NUM_UNIQUE_KEYS = config.getint('general', 'num_unique_keys')
    MAX_BROKEN_ATTRIBUTES = config.getint('general', 'max_broken_attributes') 

    #unf_output
    NAME_OUTPUT = config['unf_output']['name_output'] 
    DELIMITER = config['unf_output']['delimiter']
    ETL  = config['unf_output']['etalons']
    SRC  = config['unf_output']['sources']
    RES = config['unf_output']['result_dir']
    NUM_REC_HEADER =  config.getint('unf_output', 'num_records_header', fallback=0)
    NUM_REC_TRAILER = config.getint('unf_output', 'num_records_trailer', fallback=0)
    EXCLUDED_FIELDS = split_param(config['unf_output'].get('excluded_fields'))
    
    #fields
    HEADER_SIZE = split_param(config['unf_output'].get('field_sizes_header'))
    HEADER_NAMES = split_param(config['unf_output'].get('field_sizes_header'), str) 

    BODY_SIZE = split_param(config['unf_output'].get('field_sizes_body'))
    BODY_NAMES = split_param(config['unf_output'].get('field_names_body'), str)
    
    TRAILER_SIZE = split_param(config['unf_output'].get('field_sizes_trailer'))
    TRAILER_NAMES = split_param(config['unf_output'].get('field_names_body'), str)
   
except KeyError as e:
    print('Заполни обязательный параметр в {}! --> {}'.format(args.config_file, e))
    sys.exit()
