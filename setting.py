import configparser
import sys

INI_FILE = "config_comparison.ini"
FILENAME_FORMAT = "formats.json"
FILE_STRUCTURE = {'header':["only int"],
                  'body': {'name_field': 'length_field only int'},
                  'trailer':["only int"]}

config = configparser.ConfigParser()
config.read(INI_FILE)
try:
    
    TYPE_DELIMITER = config['general']['type_delimiter']
    PREPARE_ETL = config['general']['prepare_etalon']
    PREPARE_SRC = config['general']['prepare_source']
    MAX_LOG_FILE = int(config['general']['log_file_max_size']) 
    REGEX_RENAME = config['general']['regex_rename_files'] 

    NAME_OUTPUT = config['unf_output']['name_output'] 
    DELIMITER = config['unf_output']['delimiter']
    ETL  = config['unf_output']['etalons']
    SRC  = config['unf_output']['sources']
    RES = config['unf_output']['result_dir']
    NUM_REC_HEADER = int(config['unf_output']['num_records_header'])
    NUM_REC_TRAILER = int(config['unf_output']['num_records_trailer'])
    
    try:
        HEADER_SIZE = list(map(int, config['unf_output']['field_sizes_header'].split(',')))
    except:
        HEADER_SIZE = []
    try:
        HEADER_NAMES =  [name.strip() for name in config['unf_output']['field_names_header'].split(',')]
    except:
        HEADER_NAMES = []
    try:
        BODY_SIZE = list(map(int, config['unf_output']['field_sizes_body'].split(',')))
    except:
        BODY_SIZE = []
    try:
        BODY_NAMES =  [name.strip() for name in config['unf_output']['field_names_body'].split(',')]
    except:
        BODY_NAMES = []
    try:
        TRAILER_SIZE = list(map(int, config['unf_output']['field_sizes_trailer'].split(',')))
    except:
        TRAILER_SIZE = []
    try:
        TRAILER_NAMES =  [name.strip() for name in config['unf_output']['field_names_trailer'].split(',')]
    except:
        TRAILER_NAMES = []
except KeyError as e:
    print('Заполни обязательный параметр в {}! --> {}'.format(INI_FILE, e))
    sys.exit()