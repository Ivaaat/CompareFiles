import configparser


FILENAME_FORMAT = "formats.json"
FILE_STRUCTURE = {'header':["only int"],
                  'body': {'name_field': 'length_field only int'},
                  'trailer':["only int"]}

config = configparser.ConfigParser()
config.read("config_comparison.ini")
try:
    TYPE_DELIMITER = config['general']['type_delimiter']
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
except KeyError:
    print('Заполни обязательные поля')