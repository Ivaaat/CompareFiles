import configparser


FILENAME_FORMAT = "formats.json"
FILE_STRUCTURE = {'header':["only int"],
                  'body': {'name_field': 'length_field only int'},
                  'trailer':["only int"]}

config = configparser.ConfigParser()
config.read("config_comparison.ini")
try:
    ETL  = config['unf_output']['etalons']
    SRC  = config['unf_output']['sources']
    RES = config['unf_output']['result_dir']
    try:
        HEADER_SIZE = list(map(int, config['unf_output']['field_sizes_header'].split(',')))
    except:
        HEADER_SIZE = []
    HEADER_NAMES =  [name.strip() for name in config['unf_output']['field_names_header'].split(',')]
    try:
        BODY_SIZE = list(map(int, config['unf_output']['field_sizes_body'].split(',')))
    except:
        BODY_SIZE = []
    BODY_NAMES =  [name.strip() for name in config['unf_output']['field_names_body'].split(',')]
    try:
        TRAILER_SIZE = list(map(int, config['unf_output']['field_sizes_trailer'].split(',')))
    except:
        TRAILER_SIZE = []
    TRAILER_NAMES =  [name.strip() for name in config['unf_output']['field_names_trailer'].split(',')]
except KeyError:
    print('Заполни обязательные поля')