import codecs
import collections
import sys
from os.path import join, isfile, isdir, exists, basename
from os import mkdir, listdir
import glob
from re import search
from time import strftime, gmtime, time

# from configobj import ConfigObj
from configparser import ConfigParser
from logging import StreamHandler
import logging.handlers
import argparse


class ComparisonGroup:
    def __init__(self):
        self.record_in_etalon = 0
        self.record_in_PiOne = 0
        self.total_matched_by_id = 0
        self.in_etalon_only = 0
        self.in_PiOne_only = 0
        self.broken_attr_same_id = 0
        self.identical = 0


class Settings:
    def __init__(self):
        self.config_path = 'config_comparison.ini'
        self.log_in_file = False
        self.log_file_max_size = 100
        self.log_file_backup_count = 1
        self.system_name = None
        self.etalons = None
        self.sources = None
        self.result_dir = '.'
        self.delimiter = '|'
        self.output_delimeter = ';'
        self.key_fields = None
        self.excluded_keys = self.key_fields
        self.show_remained_entries = False
        self.use_grouping = False
        self.errors_entries = 10000
        self.field_sizes = None
        self.use_key_to_compare = False
        self.header = None
        self.extra_files_ignore = False
        self.lost_files_ignore = False
        self.use_offset = False
        self.nonunique_keys_control = False


def date_from_filename(filename):
    if filename.find('_') < 0: return ''
    filetime = filename.split('_')[1]
    return filetime[6:8] + '.' + filetime[4:6] + '.' + filetime[0:4]


def elem_comparison(arr1, arr2, excluded_positions):
    sim = 0
    diff = 0
    log_dict = {}
    all_positions_set = set(range(len(arr1)))
    excluded_positions_set = set([int(x) for x in excluded_positions])
    needed_positions = all_positions_set - excluded_positions_set
    for i in needed_positions:
        if arr1[i] != arr2[i]:
            log_dict[i] = []
            log_dict[i].append(arr1[i])
            log_dict[i].append(arr2[i])
    return log_dict


def get_key_numbers(input_string, sort=True):
    result_list = []
    keys_list = input_string.split(',')
    for key in keys_list:
        if '-' in key and type(key) != int:
            interval_list = key.split('-')
            min = int(interval_list[0])
            max = int(interval_list[1])
            new_list = range(min, max + 1)
            result_list += new_list
        elif key == '':
            return result_list
        else:
            result_list.append(int(key))

    if sort:
        return sorted(result_list)
    else:
        return result_list


def create_comparison_key(keys_list, info_list):
    try:
        return '-'.join([info_list[x] for x in keys_list])
    except:
        logger.critical(f"{keys_list} {info_list}")
        sys.exit(0)


def get_splitted_list(input_string, global_settings):
    splitted_list = []
    # print(global_settings.use_offset)
    if global_settings.use_offset:
        # positions = [0, 5, 10, 25, 33, 41, 51, 73, 95, 115, 135, 139, 143, 153, 161, 177, 178]
        # for i in range(16):
        #    splitted_list.append(input_string[positions[i]:positions[i + 1]].rstrip())

        offsets = [0]
        for size in global_settings.field_sizes:
            offsets.append(offsets[-1] + size)

        for i in range(len(offsets) - 1):
            start = offsets[i]
            end = offsets[i + 1]
            splitted_list.append(input_string[start:end])
    else:
        splitted_list = input_string.strip().split(global_settings.delimiter)

    return splitted_list


def create_record_from_list(input_string, delimiter):
    return delimiter.join(list(map(str, input_string))) + '\n'


def format_time(in_time):
    return strftime('%Y%m%d_%H%M%S', gmtime(in_time))

start_time = time()

# -----------------------------
# --- INIT

# global objects
settings = Settings()
group_by_list = list()

# flags
use_arguments_settings = False
use_config_settings = False

# init logger
logger = logging.getLogger('logger')
logger.setLevel(logging.DEBUG)
stream_handler = StreamHandler(stream=sys.stdout)
logger.addHandler(stream_handler)
stream_formatter = logging.Formatter('%(levelname)s. %(message)s')
file_formatter = logging.Formatter('%(asctime)s: %(levelname)s. %(message)s')
stream_handler.setFormatter(stream_formatter)

# default values
# log_in_file = False
# log_file_max_size = 100
# log_file_backup_count = 1

# init arguments
parser = argparse.ArgumentParser(description='Text file comparison tool',
                                 epilog='Please, specify the -f, --format FORMAT from the configuration file OR directories with etalons (-e, --etalons ETALON_DIR) AND sources (-s, --sources SOURCE_DIR)')

parser.add_argument('-c, --config', help='set config file', dest='config_file', default='config_comparison.ini')
parser.add_argument('-f, --format', help='format from config file ( ignore if -e and -s)', dest='format')
parser.add_argument('-e, --etalons', help='dir with etalon files   (needed if -s)', dest='etalons')
parser.add_argument('-s, --sources', help='dir with source files   (needed if -e)', dest='sources')
parser.add_argument('-o, --out', help=' result output dir (if -e and -e, default cur_dir, ignore if format)', dest='result_dir')
parser.add_argument('-d, --delimiter', help='use delimiter(csv(;) default)', dest='delimiter')
parser.add_argument('-z, --sizes', help='use this offsets (ex: 4,4,4,4,4,4,4,) ( if use -d ignore)', dest='sizes')
parser.add_argument('-k, --keys', help='use this key fields (if no key_fields line by line)', dest='key_fields')
parser.add_argument('-x, --exclude', help='exclude from compare ( keys excluded )', dest='excluded_keys')
parser.add_argument('-h, --header', help='header', dest='header')
# parser.add_argument('-r, --report', help='report level 0 - ', dest='report')
parser.add_argument('-l, --log', help='log to file', dest='log')
parser.add_argument('-g, --loglevel', help='log level', dest='loglevel', choices=[0, 10, 20, 30, 40, 50], type=int)
parser.add_argument('-n, --nunique', help='nonunique keys makes unique', const=1, action='store_const', dest='nunique')
parser.add_argument('-r, --remained', help='show remained entries', const=1, action='store_const', dest='remained')

args = parser.parse_args()

# check for required arguments
if not args.format and (not args.etalons or not args.sources):
    parser.print_help()
    sys.exit(1)
elif args.format and args.etalons and args.sources:
    logger.warning('Argument -f, --format FORMAT ignored. -e, --etalons ETALON_DIR AND -s, --sources SOURCE_DIR arguments detected')

# choice of settings source
if args.etalons and args.sources:
    logger.debug('Set settings values from arguments')
    use_arguments_settings = True
elif args.format and (not args.etalons and not args.sources) and args.config_file:
    logger.debug(f"Set settings values from config file {args.config_file}")
    use_config_settings = True
else:
    logger.error("Somthing wrong with argument. Please check.")
    logger.debug(str(args))
    parser.print_help()
    sys.exit(1)

# settings set
settings.config_path = args.config_file

#print(f"use config: {use_config_settings}")
if use_config_settings:
    if not exists(settings.config_path):
        logger.error(f"Config file {settings.config_path} not found!")
        sys.exit(1)

    config = ConfigParser()
    config.read(settings.config_path)
    # configspec = ConfigObj(settings.config_path, interpolation=False, list_values=False, _inspec=True)
    # config = ConfigObj(settings.config_path, list_values=False, configspec=configspec)
    settings.log_in_file            = config['general'].getboolean('log_in_file')
    settings.log_file_max_size      = int(config['general']['log_file_max_size'])
    settings.log_file_backup_count  = int(config['general']['log_file_backup_count'])
    settings.system_name            = args.format
    if settings.system_name not in config:
        logger.error(f'Format not found in config file {settings.config_path}')
        sys.exit(1)
    if 'etalons' not in config[settings.system_name]:
        logger.error('etalons not found in config')
        sys.exit(1)
    settings.etalons = config[settings.system_name]['etalons']
    if 'sources' not in config[settings.system_name]:
        logger.error('sources not found in config')
        sys.exit(1)
    settings.sources = config[settings.system_name]['sources']
    settings.result_dir = config[settings.system_name]['result_dir']
    settings.delimiter = config[settings.system_name]['delimiter']
    if 'key_fields' not in config[settings.system_name]:
        settings.use_key_to_compare = False
    else:
        settings.use_key_to_compare = True
        settings.key_fields = config[settings.system_name]['key_fields']
        settings.excluded_keys = settings.key_fields
    if 'excluded_fields' in config[settings.system_name]:
        settings.excluded_keys += ',' + config[settings.system_name]['excluded_fields']
    settings.show_remained_entries = config[settings.system_name]['show_remained_entries']
    if config[settings.system_name].get('use_grouping') == 'True':
        settings.use_grouping = True
    settings.errors_entries = int(config[settings.system_name]['entries_in_error_files'])
    if settings.use_grouping and 'group_by_fields' in config[settings.system_name]:
        group_by_list = get_key_numbers(config[settings.system_name]['group_by_fields'])
    elif settings.use_grouping and not 'group_by_fields' in config[settings.system_name]:
        settings.use_grouping = False
        logger.warning('Grouping fields not defined.')
    if 'field_sizes' in config[settings.system_name]:
        settings.field_sizes = get_key_numbers(config[settings.system_name]['field_sizes'])
        settings.use_offset = True
    settings.lost_files_ignore = config['general'].getboolean('lost_files_ignore')
    settings.extra_files_ignore = config['general'].getboolean('extra_files_ignore')
    settings.nonunique_keys_control = config['general'].getboolean('nonunique_keys_control')

#print(f"use arguments: {use_arguments_settings}")
if use_arguments_settings:
    settings.etalons = args.etalons
    settings.sources = args.sources
    settings.system_name = args.format

    if args.result_dir:
        settings.result_dir = args.result_dir
    if args.delimiter:
        settings.delimiter = args.delimiter
    if args.sizes:
        settings.field_sizes = get_key_numbers(args.sizes, False)
        settings.use_offset = True
    if args.key_fields:
        settings.key_fields = args.key_fields
        settings.use_key_to_compare = True
        settings.excluded_keys = args.key_fields
    if args.excluded_keys:
        settings.excluded_keys += ',' + args.excluded_keys
    if args.header:
        settings.header += ',' + args.header
    if args.log:
        settings.log_in_file = True
        if args.loglevel:
            logger.setLevel(args.loglevel)
    if args.nunique:
        settings.nonunique_keys_control = True
    if args.remained:
        settings.show_remained_entries = True

# check dirs:
if (isdir(settings.etalons) and not isdir(settings.sources)) or (
        not isdir(settings.etalons) and isdir(settings.sources)):
    logger.error('Please, compare dir with dir, or file with file')
    sys.exit(1)


if settings.key_fields is not None:
    key_list_string = settings.key_fields
    excluded_keys_string = settings.excluded_keys
logger.debug(f"sysName {settings.system_name}")
statistic_model_dir = f"{join(settings.result_dir, settings.system_name)}_diff_{format_time(start_time)}"

# File names
comparison_result_filename = f"{settings.system_name}_{format_time(start_time)}_Comparison_Result.csv"
error_number_filename = f"{settings.system_name}_{format_time(start_time)}_Error_Number.csv"

if not exists(statistic_model_dir):
    mkdir(statistic_model_dir)

if settings.log_in_file:
    file_handler = logging.handlers.RotatingFileHandler(
        f"{join(statistic_model_dir, settings.system_name)}_{format_time(start_time)}.log",
        mode='a',
        maxBytes=settings.log_file_max_size * 1024 * 1024,
        backupCount=settings.log_file_backup_count)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

# if '\\' in settings.delimiter:
#     settings.delimiter = str(codecs.decode(settings.delimiter, 'unicode_escape'))

# key_list = get_key_numbers(key_list_string)
# excluded_key_list = get_key_numbers(excluded_keys_string)
key_list = list(map(int, key_list_string.split(',')))
excluded_key_list = list(map(int, excluded_keys_string.split(',')))
total_error_dict = dict()
errors_separate_dict = dict()
file_counter = len(listdir(settings.sources))  # len(glob.glob(settings.sources + '*.src'))
# print 'Number of comparing files: ' + str(file_counter)
logger.info(f"Number of comparing files: {file_counter}")

similar_files_list = []
source_only_files_list = []
source_list = []
source_dict = dict()
etalon_list = []
etalon_dict = dict()
nonUniqueList = []

groups_dict = dict()

in_etalon_total = 0
in_src_total = 0
matched_keys_total = 0
in_etalon_only_total = 0
in_src_only_total = 0
full_matched_total = 0
partially_matched_total = 0
comparison_header = 'Comparing file;Date;Group by;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Broken attributes same ID;Identical'

if settings.field_sizes is not None:
    logger.debug(settings.field_sizes)

comp_res_filename = join(statistic_model_dir, comparison_result_filename)
with open(comp_res_filename, "a+") as total:
    total.write(f"{comparison_header}\n")

# Checking all similar filenames:
for file in listdir(settings.sources):  # glob.glob(settings.source + '*'):
    filename = basename(file)
    # filename = re.sub(r'\.', '', filename)
    if isfile(join(settings.etalons, filename)):
        similar_files_list.append(filename)
    elif not settings.extra_files_ignore:
        file_counter -= 1
        # print filename + ' not found in etalon files. Remained files: ' + str(file_counter)
        logger.warning(f"{filename} not found in etalon files. Remained files: {file_counter}")
        with open(file, "r+") as src:
            line_count = sum(1 for _ in src)
            in_src_total += line_count
            in_src_only_total += line_count
            # Writing all files from src only to the comparison result
            with open(comp_res_filename, "a+") as total:
                total.write(f"{filename};{date_from_filename(filename)};TOTAL;0;{line_count};0;0;{line_count};0;0\n")

# Check files only in etalon and write them to statistic
for file in listdir(settings.etalons):  # glob.glob( + '*'):
    etalon_filename = basename(file)
    # etalon_filename = re.sub(r'\.etalon', '', etalon_filename)
    if etalon_filename not in similar_files_list and not settings.lost_files_ignore:
        # print etalon_filename + ' not found in PiOne!'
        logger.info(f"{etalon_filename} not found in sources!")
        with open(join(settings.etalons, etalon_filename)) as etl:
            line_count = sum(1 for _ in etl)
            in_etalon_total += line_count
            in_etalon_only_total += line_count
            # Writing all files from src only to the comparison result
            with open(comp_res_filename, "a+") as total:
                total.write(f"{etalon_filename};{date_from_filename(filename)};TOTAL;{line_count};0;0;{line_count};0;0;0\n")

# Working with files with same names
for file in listdir(settings.sources):  # glob.glob(settings.source + '*'):
    filename = basename(file)
    # filename = re.sub(r'\.src', '', filename)
    if filename in similar_files_list:
        source_dict.clear()
        etalon_dict.clear()
        groups_dict.clear()
        del source_list[:]
        del etalon_list[:]

        # Filling source dict by src file
        with codecs.open(join(settings.sources, filename), "r+", encoding='UTF-8') as f:
            lines = f.readlines()
            for line in lines:
                # print(line)
                line_arr = get_splitted_list(line, settings)
                # print(line_arr)
                key = create_comparison_key(key_list, line_arr)
                source_list.append(key)
                # get non-unique keys
                if settings.nonunique_keys_control and key in source_dict:
                    nonUniqueList.append(key)
                    newKey = key + '$1'
                    while newKey in source_dict:
                        newId = int(newKey.split('$')[1]) + 1
                        newKey = key + '$' + str(newId)
                    source_dict[newKey] = line_arr
                else:
                    source_dict[key] = line_arr

                # Group for comparison result
                if settings.use_grouping:
                    field_group_by = create_comparison_key(group_by_list, line_arr)
                    if field_group_by not in groups_dict:
                        groups_dict[field_group_by] = ComparisonGroup()
                        groups_dict[field_group_by].record_in_PiOne = 1
                    else:
                        groups_dict[field_group_by].record_in_PiOne += 1

        # Filling etalon dict by etalon file
        with codecs.open(join(settings.etalons, filename), "r+", encoding='UTF-8') as f:
            lines = f.readlines()
            for line in lines:
                line_arr = get_splitted_list(line, settings)
                key = create_comparison_key(key_list, line_arr)
                etalon_list.append(key)
                # etalon_dict[key] = line_arr

                # get non-unique keys
                if settings.nonunique_keys_control and key in etalon_dict:
                    # nonUniqueList.append(key)
                    newKey = key + '$1'
                    while newKey in etalon_dict:
                        newId = int(newKey.split('$')[1]) + 1
                        newKey = key + '$' + str(newId)
                    etalon_dict[newKey] = line_arr
                else:
                    etalon_dict[key] = line_arr

                # Group for comparison result
                if settings.use_grouping:
                    field_group_by = create_comparison_key(group_by_list, line_arr)
                    if field_group_by not in groups_dict:
                        groups_dict[field_group_by] = ComparisonGroup()
                        groups_dict[field_group_by].record_in_etalon = 1
                    else:
                        groups_dict[field_group_by].record_in_etalon += 1

        file_counter -= 1

        source_set = set(source_dict)
        etalon_set = set(etalon_dict)
        similar = (source_set & etalon_set)
        in_source_only = source_set - similar
        in_etalon_only = etalon_set - similar

        source_counter = len(source_set)
        etalon_counter = len(etalon_set)
        similar_counter = len(similar)
        in_source_counter = len(in_source_only)
        in_etalon_counter = len(in_etalon_only)

        logger.info(f"Comparing file: {filename} (file remained = {file_counter})")
        if len(source_list) > len(source_dict):
            logger.error('Non-unique keys found. Please check key fields.')

        logger.debug(f"First key look like: {source_list[0]}")
        logger.info(f"Matched keys: {similar_counter}. Only in PiOne: {in_source_counter}. Only in Etalon: {in_etalon_counter}")

        if settings.show_remained_entries:
            if len(in_source_only) > 0:
                with open(f"{join(statistic_model_dir, filename)}.src", 'a+') as src:
                    # print(source_dict)
                    for elem in in_source_only:
                        if settings.use_grouping:
                            field_group_by = create_comparison_key(group_by_list, source_dict[elem])
                            groups_dict[field_group_by].in_PiOne_only += 1
                        new_entry = create_record_from_list(source_dict.pop(elem), settings.delimiter)
                        src.write(new_entry)

            if len(in_etalon_only) > 0:
                with open(f"{join(statistic_model_dir, filename)}.etalon", 'a+') as etl:
                    for elem in in_etalon_only:
                        if settings.use_grouping:
                            field_group_by = create_comparison_key(group_by_list, etalon_dict[elem])
                            groups_dict[field_group_by].in_etalon_only += 1
                        new_entry = create_record_from_list(etalon_dict.pop(elem), settings.delimiter)
                        etl.write(new_entry)

        elif settings.use_grouping:
            for elem in in_source_only:
                field_group_by = create_comparison_key(group_by_list, source_dict[elem])
                groups_dict[field_group_by].in_PiOne_only += 1

            for elem in in_etalon_only:
                field_group_by = create_comparison_key(group_by_list, etalon_dict[elem])
                groups_dict[field_group_by].in_etalon_only += 1

        # Comparing entries with same keys:
        full_matched_counter = 0
        partially_matched_counter = 0

        if settings.use_key_to_compare:
            for key in similar:

                # Group for comparsion result
                if settings.use_grouping:
                    field_group_by = create_comparison_key(group_by_list, source_dict[key])
                    groups_dict[field_group_by].total_matched_by_id += 1

                result = elem_comparison(source_dict[key], etalon_dict[key], excluded_key_list)

                if len(result) == 0:
                    full_matched_counter += 1
                    # Group for comparsion result
                    if settings.use_grouping:
                        groups_dict[field_group_by].identical += 1
                else:
                    partially_matched_counter += 1
                    # Group for comparsion result
                    if settings.use_grouping:
                        groups_dict[field_group_by].broken_attr_same_id += 1

                for error in result:
                    error_name = f"{error};TOTAL"
                    if error_name not in total_error_dict:
                        total_error_dict[error_name] = 1
                    else:
                        total_error_dict[error_name] += 1
                    # Error_Number.csv. Group errors by fields
                    if settings.use_grouping:
                        group_error_name = f"{error}';'{field_group_by}"
                        if group_error_name not in total_error_dict:
                            total_error_dict[group_error_name] = 1
                        else:
                            total_error_dict[group_error_name] += 1

                    if not settings.use_grouping:
                        if settings.errors_entries == 0 or total_error_dict[error_name] > settings.errors_entries:
                            pass
                        elif settings.errors_entries == -1 or total_error_dict[error_name] <= settings.errors_entries:
                            with codecs.open(
                                    f"{join(statistic_model_dir, settings.system_name)}_{format_time(start_time)}_field_{error}.report",
                                    'a+',
                                    encoding='UTF-8') as err:
                                err.write(f"SRC: {settings.delimiter.join(source_dict[key])}\n")
                                err.write(f"ETL: {settings.delimiter.join(etalon_dict[key])}\n\n")
                                err.write(f"{result[error][0].rstrip()};{result[error][1].rstrip()}\n\n")
                    else:
                        if settings.errors_entries == 0 or total_error_dict[group_error_name] > settings.errors_entries:
                            pass
                        elif settings.errors_entries == -1 or total_error_dict[
                            group_error_name] <= settings.errors_entries:
                            with open(f"{join(statistic_model_dir, settings.system_name)}_{format_time(start_time)}_field_{error}_group_{field_group_by}.report",
                                      'a+') as err:
                                # err.write('SRC: ' + '\t'.join(source_dict[key]) + '\n')
                                err.write(f"SRC: {settings.delimiter.join(source_dict[key])}\n")
                                err.write(f"ETL: {settings.delimiter.join(etalon_dict[key])}\n\n")
                                err.write(f"{result[error][0].rstrip()};{result[error][1].rstrip()}\n\n")

        in_etalon_total += etalon_counter
        in_src_total += source_counter
        matched_keys_total += similar_counter
        in_etalon_only_total += in_etalon_counter
        in_src_only_total += in_source_counter
        partially_matched_total += partially_matched_counter
        full_matched_total += full_matched_counter

        # Writing statistics to the result file
        with open(comp_res_filename, "a+") as total:
            if settings.use_grouping:
                for elem in groups_dict:
                    total.write(create_record_from_list([filename,
                        date_from_filename(filename),
                        elem,
                        groups_dict[elem].record_in_etalon,
                        groups_dict[elem].record_in_PiOne,
                        groups_dict[elem].total_matched_by_id,
                        groups_dict[elem].in_etalon_only,
                        groups_dict[elem].in_PiOne_only,
                        groups_dict[elem].broken_attr_same_id,
                        groups_dict[elem].identical], settings.output_delimeter))

            total.write(create_record_from_list([filename,
                date_from_filename(filename),
                'TOTAL',
                etalon_counter,
                source_counter,
                similar_counter,
                in_etalon_counter,
                in_source_counter,
                partially_matched_counter,
                full_matched_counter], settings.output_delimeter))

# Writing summary to the result file
with open(comp_res_filename, "a+") as total:
    total.write(create_record_from_list(['TOTAL',
        '',
        '',
        in_etalon_total,
        in_src_total,
        matched_keys_total,
        in_etalon_only_total,
        in_src_only_total,
        partially_matched_total,
        full_matched_total], settings.output_delimeter))

with open(comp_res_filename, "a+") as total:
    # write header in the end without first column
    total.write(comparison_header.replace(search(r'^[\w\s]*', comparison_header).group(0),''))

# Writing info about errors to the file
with open(comp_res_filename, "a+") as err:
    # Sorting for pretty out
    sorted_by_key_total_error_dict = collections.OrderedDict(sorted(total_error_dict.items()))
    sorted_by_error_error_dict = collections.OrderedDict(
        sorted(sorted_by_key_total_error_dict.items(), key=lambda v: int(v[0].split(";")[0])))
    err.write('Row with error;Group by;Errors number\n')
    for error in sorted_by_error_error_dict:
        err.write(f"{error};{sorted_by_error_error_dict[error]}\n")

end_time = time()
processing_time = end_time - start_time
if processing_time < 1:
    # print 'Processing time: ' + str(processing_time) + ' seconds'
    logger.info(f"Processing time: {processing_time} seconds")
else:
    # print 'Processing time: ' + time.strftime('%H:%M:%S', time.gmtime(processing_time))
    logger.info(f"Processing time: {strftime('%H:%M:%S', gmtime(processing_time))}")