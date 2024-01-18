import configparser


FILENAME_FORMAT = "formats.json"
FILE_STRUCTURE = {'header':["only int"],
                  'body': {'name_field': 'length_field only int'},
                  'trailer':["only int"]}

config = configparser.ConfigParser()
config.read("config_comparison.ini")
ETL  = config['unf_output']['etalons']
SRC  = config['unf_output']['sources']
RES = config['unf_output']['result_dir']
HEADER_SIZE = list(map(int, config['unf_output']['field_sizes_header'].split(',')))
BODY_SIZE = list(map(int, config['unf_output']['field_sizes_body'].split(',')))
BODY_NAMES =  [name.strip() for name in config['unf_output']['field_names_body'].split(',')]
TRAILER_SIZE = list(map(int, config['unf_output']['field_sizes_trailer'].split(',')))