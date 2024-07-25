import setting
import csv
from datetime import datetime
import codecs
from os.path import join, isfile, isdir, exists
from concurrent.futures import ThreadPoolExecutor
import os
from collections import defaultdict
import logging
import multiprocessing
import time
import re
import shutil
from logging import StreamHandler
import logging.handlers
import sys
import glob
from abc import ABC, abstractmethod






class File(ABC):

    names_files = {}
    rename = False
    
    @abstractmethod
    def __init__(self, path) -> None:
        self.path = path
        self.isdir = isdir(self.path)
        self.isfile = isfile(self.path)
        self.regex = setting.REGEX_RENAME
        self.new_path = ''


    def get_files_dir(self):
        self.path = os.path.abspath(self.path)
        self.path_filenames = glob.glob(join(self.path, self.mask_files))
        self.filenames = [os.path.split(file)[1] for file in self.path_filenames]
    

    def сheck_and_rename_files(self):
        Logger.logger.info(f"Check name files {str(self)}")
        i = 0
        for i, file in enumerate(self.path_filenames):
            filename = os.path.split(file)[1]
            if isfile(file):
                i+=1
                print(f'Check {i} = {file}')
                if File.rename:
                    try:
                        file_name = ''.join(re.findall(self.regex, filename)[0])
                    except IndexError:
                        Logger.logger.error('Regexp not match with the file {}.'.format(filename))
                        ReportAll.create_error_file('Regexp not match in {}'.format(str(self)) , filename)
                        print(f'Error regex in {file}')
                        continue
                else:
                    file_name = os.path.split(file)[1]
                if file_name not in File.names_files:
                    File.names_files[file_name] = {'etl': [], 'src': []}
                File.names_files[file_name][str(self)].append(file)
                


    def _create_new_folder(self) :
        self.new_path = '{}_{}_compare'.format(self.path, re.sub(r'[\\/*?:"<>|]',"", self.mask_files))
        if not exists(self.new_path):
            os.mkdir(self.new_path)
        else:
            ask = input(f'Delete {self.new_path} folder? y/n: ')
            if ask == 'y':
                shutil.rmtree(self.new_path)
                os.mkdir(self.new_path)
            elif ask == 'n':
                pass
            else:
                self._create_new_folder()
   


class Etalon(File):
    
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.prepare = setting.PREPARE_ETL
        self.encode = setting.ENCODE_FILES_ETALON
        self.mask_files = setting.MASK_FILES_ETALON
        

    def __str__(self) -> str:
        return 'etl'
    

class Source(File):

    
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.prepare = setting.PREPARE_SRC
        self.encode = setting.ENCODE_FILES_SOURCE
        self.mask_files = setting.MASK_FILES_SOURCE
    

    def __str__(self) -> str:
        return 'src'


class Collect:
    def __init__(self, etalon: Etalon, source: Source) -> None:
        self.etalon = etalon
        self.source = source
    

class CollectFiles(Collect):

    def __init__(self, etalon: Etalon, source: Source) -> None:
        super().__init__(etalon, source)
    

    def get_files(self):
        etalon.get_files_dir()
        source.get_files_dir()
        if (set(etalon.filenames) - set(source.filenames)):
            File.rename = True 
            etalon._create_new_folder()
            source._create_new_folder()
        etalon.сheck_and_rename_files()
        source.сheck_and_rename_files()
    
    

class CollectFile(Collect):

    def __init__(self, etalon: Etalon, source: Source) -> None:
        super().__init__(etalon, source)

    def get_files(self):
        filename_etl = os.path.split(etalon.path)[1]
        filename_src = os.path.split(source.path)[1]
        File.names_files = {'{} | {}'.format(filename_etl, filename_src)
                                            : {'etl': [etalon.path], 'src':[source.path]}}
        

class FilesFactory:
    def create_collect(self, etalon: Etalon, source: Source):
        if etalon.isdir and source.isdir:
            return CollectFiles(etalon, source)
        elif etalon.isfile and source.isfile:
            return CollectFile(etalon, source)
        else:
            print("Сравнить можно либо папки, либо файлы, проверь пути в config_comparison.ini")
            sys.exit()


class Counter:
    
    total_dict = {
                'record_len_etl':0, 
                'record_len_src' : 0,
                'record_matched': 0,
                'record_only_in_etl' : 0,
                'record_only_in_src' : 0,
                'record_repeated_in_etl' : 0,
                'record_repeated_in_src' : 0,
                'record_broken_attributes' : 0,
                'record_indentical' : 0,
                }


    def __init__(self) -> None:
        
        self.record_len_etl = 0
        self.record_len_src = 0
        self.record_matched = 0
        self.record_only_in_etl = 0
        self.record_only_in_src = 0
        self.record_repeated_in_etl = 0
        self.record_repeated_in_src = 0
        self.record_broken_attributes = 0
        self.record_indentical = 0
        self.errors = {}


    

    def set_attr(self, attr, buff):
        new_val = getattr(self, attr) + buff
        setattr(self, attr, new_val)
        Counter.total_dict[attr] += buff
    

class FileReader:

    def __init__(self, filenames, prepare, encode):
        self.records = []
        for file in filenames:
            with codecs.open(file, 'r', encoding=encode) as f:
                self.records += f.readlines() if not prepare else f.readlines()[setting.NUM_REC_HEADER:setting.NUM_REC_TRAILER]
                




class PartsFactory:
    
    full_compare = False

    def __init__(self, reader: FileReader, new_path, filename_new: str) -> None:
        self.reader = reader 
        self.new_path = new_path
        self.len_records = len(self.reader.records)
        self.set_records = set(self.reader.records)
        self.len_set_records = len(self.set_records)
        self.filename_new = filename_new
        if File.rename:
            self.prepare_records()
        self._create_parts_document()
        

    
    def prepare_records(self):
        with open(join(self.new_path, self.filename_new), "w+", encoding='UTF-8') as file:
            for rec in self.reader.records:
                file.write(rec.strip() + '\n')
        
    


    def _create_parts_document(self):
        if setting.HEADER_SIZE and setting.BODY_SIZE and setting.TRAILER_SIZE:
            self.header = Header(self.reader.records[:setting.NUM_REC_HEADER], setting.NUM_REC_HEADER)
            self.body = Body(self.reader.records[setting.NUM_REC_HEADER:-setting.NUM_REC_TRAILER], setting.NUM_REC_HEADER + 1)
            self.trailer = Trailer(self.reader.records[-setting.NUM_REC_TRAILER:], self.len_records - setting.NUM_REC_TRAILER + 1)
            PartsFactory.full_compare = True
        else:
            self.body = Body(self.reader.records, 1)



            


class InitParts:
    def __init__(self, records : list, start : int) -> None:
        self.start = start
        self.records = []
        self.len_records = len(records)
        self.records_for_compare = []
        self.delimiter = setting.DELIMITER
        self.repeated_elements = {}
        self.num_record_dict = {}
        self.diff_res =  set()
        self.compare_list = []
        line_numbers = {}  # Словарь для хранения номеров строк
        line_counts = {}   # Словарь для подсчета повторений строк
       
        for i, record in enumerate(records, start):
            record = record.strip()
            if record:
                self.records.append(record)
            #if self.records.count(record) > 1:
                # self.repeated_elements[record] = line_counts.get(record, 0) + 1
                # try:
                #     if self.repeated_elements[record] > 1:
                # #if record in self.repeated_elements:
                #         self.repeated_elements[record] = line_counts.get(record, 0) + 1
                # #self.repeated_elements[record] = self.records.count(record)
                #         continue
                # except KeyError:
                #     self.repeated_elements[record] = line_counts.get(record, 0) + 1
                # self.records_for_compare.append((i, record))
                self.num_record_dict[record] = i
        self.set_records = set(self.records)
        self.len_repeat_records =  self.len_records - len(self.set_records)
    

    def _split_record(self):
        if setting.TYPE_DELIMITER == 'char':
            self.__split_record_delimeter()
        elif setting.TYPE_DELIMITER == 'fields':
            self.__split_record_fields()


    def __split_record_delimeter(self):
        for record in self.diff_res:
            split_record = record.split(self.delimiter)
            records_list = []
            for field in split_record:
                records_list.append(field.strip())
            self.compare_list.append((self.num_record_dict[record],records_list))
    

    def __split_record_fields(self):
        for record in self.diff_res:
            records_list = []
            start = 0
            for i, field in enumerate(self.size_fields):
                records_list.append(record[start:start + field].strip())
                start+=field
            self.compare_list.append((self.num_record_dict[record], records_list))
    
    def __exclude_fields(self):
        for record in self.diff_res:
            records_list = []
            start = 0
            for i, field in enumerate(self.size_fields):
                if i in setting.EXCLUDED_FIELDS:
                     continue
                records_list.append(record[start:start + field].strip())
                start+=field
            self.compare_list.append((self.num_record_dict[record], records_list))
        self.compare_list = self.compare_list.copy()    
        
class Header(InitParts):

    def __init__(self, records : list, start : int) -> None:
        super().__init__(records, start)
        self.size_fields = setting.HEADER_SIZE
        self.name_fields = setting.HEADER_NAMES
        self.name_part = 'header'


class Body(InitParts):


    def __init__(self, records : list, start : int) -> None:
        super().__init__(records, start)
        self.size_fields = setting.BODY_SIZE
        self.name_fields = setting.BODY_NAMES
        self.name_part = 'body'


class Trailer(InitParts):


    def __init__(self, records : list, start : int) -> None:
        super().__init__(records, start)
        self.size_fields = setting.TRAILER_SIZE
        self.name_fields = setting.TRAILER_NAMES
        self.name_part = 'trailer'


class RecordSeparation:

    def __init__(self, etl: PartsFactory, src: PartsFactory, counter: Counter) -> None:
        self.etl = etl
        self.src = src
        self.counter = counter
        self.counter.set_attr('record_len_etl', etl.len_records)
        self.counter.set_attr('record_len_src', src.len_records)
        self.intersection = {}
        

    def difference_types(self):
        if PartsFactory.full_compare:
            self._difference_part(self.etl.header, self.src.header)
            self._difference_part(self.etl.body, self.src.body)
            self._difference_part(self.etl.trailer, self.src.trailer)
        else:
            self._difference_part(self.etl.body, self.src.body)
    

    def _check_repeat_records(self, etl_part: InitParts, src_part: InitParts):
        if etl_part.repeated_elements:
            for item in etl_part.diff_res:
                try:
                    self.counter.set_attr('record_repeated_in_etl', etl_part.repeated_elements[item])
                    self.counter.set_attr('record_only_in_etl', etl_part.repeated_elements[item] - 1)
                except KeyError:
                    continue
        if src_part.repeated_elements:
            for item in src_part.diff_res:
                try:
                    self.counter.set_attr('record_repeated_in_src', src_part.repeated_elements[item])
                    self.counter.set_attr('record_only_in_src', src_part.repeated_elements[item] - 1)
                except KeyError:
                    continue
        intersection_repeat = etl_part.repeated_elements.keys() & src_part.repeated_elements.keys()
        if intersection_repeat:
            for item in intersection_repeat:
                try:
                    self.counter.set_attr('record_matched',etl_part.repeated_elements[item] - 1)
                    self.counter.set_attr('record_indentical', etl_part.repeated_elements[item] - 1)
                except KeyError:
                    continue

    def _difference_part(self, etl_part: InitParts, src_part: InitParts):
        self.counter.set_attr('record_repeated_in_etl', etl_part.len_repeat_records)
        self.counter.set_attr('record_repeated_in_src', src_part.len_repeat_records)
        etl_part.diff_res = etl_part.num_record_dict.keys() - src_part.num_record_dict.keys() 
        src_part.diff_res = src_part.num_record_dict.keys() - etl_part.num_record_dict.keys() 
        self.intersection = etl_part.num_record_dict.keys() & src_part.num_record_dict.keys()
        #if setting.EXCLUDED_FIELDS:
             #etl_part._clear_split_record()
             #src_part._clear_split_record()
        self.counter.set_attr('record_matched', len(self.intersection))
        self.counter.set_attr('record_indentical', len(self.intersection))
        self.counter.set_attr('record_only_in_etl', len(etl_part.diff_res))
        self.counter.set_attr('record_only_in_src', len(src_part.diff_res))
        #self._check_repeat_records(etl_part, src_part)
        etl_part._split_record()
        src_part._split_record()


class PartsComparison:
    def __init__(self, etl: PartsFactory, src: PartsFactory, counter: Counter):
        self.etl = etl
        self.src = src
        self.counter = counter


    def execute(self):
        if PartsFactory.full_compare:
            self._compare_fields(self.etl.header, self.src.header)
            self._compare_fields(self.etl.body, self.src.body)
            self._compare_fields(self.etl.trailer, self.src.trailer)
        else:
            self._compare_fields(self.etl.body, self.src.body)


    def _compare_fields(self, etl_part: InitParts, src_part: InitParts):
        self.counter.errors[etl_part.name_part] = defaultdict(list)
        max_broken_list = []
        for src_rec in  src_part.compare_list.copy():
            broken_records = []
            for etl_rec in etl_part.compare_list.copy():
                diff_field = []
                for i in range(len(etl_part.compare_list[0][1])):
                    if i in setting.EXCLUDED_FIELDS:
                        continue
                    try:
                        if src_rec[1][i] != etl_rec[1][i]:
                            diff_field.append(i)
                    except IndexError:
                        print('!!!! Разное количество полей')
                        sys.exit()
                if len(diff_field) <= setting.NUM_UNIQUE_KEYS and len(diff_field) > 0:
                    broken_records.append([etl_rec, diff_field])
                elif not diff_field:
                    self.counter.set_attr('record_matched', 1)
                    self.counter.set_attr('record_indentical', 1)
                    etl_part.compare_list.remove(etl_rec)
                    self.counter.set_attr('record_only_in_etl', -1)
                    src_part.compare_list.remove(src_rec)
                    self.counter.set_attr('record_only_in_src', -1)
                    broken_records = []
                    break
            if broken_records:
                     broken_records.sort(key=lambda x: len(x[1]))
                     src_part.compare_list.remove(src_rec)
                     self.counter.set_attr('record_only_in_src', -1)
                     etl_part.compare_list.remove(broken_records[0][0])
                     self.counter.set_attr('record_matched', 1)
                     self.counter.set_attr('record_only_in_etl', -1)
                     for field in broken_records[0][1]:
                       #self.counter.set_attr('record_broken_attributes', 1)
                       if len(self.counter.errors[etl_part.name_part][field]) > setting.MAX_BROKEN_ATTRIBUTES:
                            if field not in max_broken_list:
                                max_broken_list.append(field)
                                Logger.logger.info(f"{self.src.filename_new}. THE MAXIMUM VALUE OF ATTRIBUTES IS EXCEEDED \"MAX_BROKEN_ATTRIBUTES\" Name: {etl_part.name_fields[field]}  Num: {field}")
                            continue
                       self.counter.set_attr('record_broken_attributes', 1)
                       self.counter.errors[etl_part.name_part][field].append((broken_records[0][0], src_rec))
                       try:
                           name_field = '{}'.format(etl_part.name_fields[field])
                       except IndexError:
                           name_field = '{}'.format(field)
                       Counter.total_dict[name_field]+=1


        


class ReportAll:
    date = datetime.now().strftime("%d-%m-%y_%H%M%S")
    comparison_header = 'Comparing file;Date;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Records repeat in etl;Records repeat in src;Broken attributes same ID;Identical\n'
    result_path = '{}_{}_Comparison_Result.csv'.format(setting.NAME_OUTPUT, date)
    if not exists(setting.RES):
        os.mkdir(setting.RES)
    path_folder = join(setting.RES, '{}_diff_{}'.format(setting.NAME_OUTPUT,  date))
    if not exists(path_folder):
        os.mkdir(path_folder)
    with open(join(path_folder, result_path), 'w+')  as csv_file: 
        csv_file.write(comparison_header)


    def __init__(self, etl: PartsFactory, src: PartsFactory, counter: Counter, filename: str) -> None:
        self.etl = etl
        self.src = src
        self.counter = counter
        self.filename = filename
        
    def create_file_errors_report(self):
        if PartsFactory.full_compare:
            self.create_part_file_errors_report(self.etl.header, self.src.header)
            self.create_part_file_errors_report(self.etl.body, self.src.body)
            self.create_part_file_errors_report(self.etl.trailer, self.src.trailer)
        else:
            self.create_part_file_errors_report(self.etl.body, self.src.body)
        
    
    
    def write_file_report_csv(self):
        with open(join(self.path_folder, self.result_path), 'a+') as csv_file: 
            csv_writer = csv.writer(csv_file, delimiter = ';')
            csv_writer.writerow([self.filename, 
                                datetime.now().strftime("%d-%m-%y_%H%M%S"),
                                self.etl.len_records,
                                self.src.len_records,
                                self.counter.record_matched,
                                self.counter.record_only_in_etl,
                                self.counter.record_only_in_src,
                                self.counter.record_repeated_in_etl,
                                self.counter.record_repeated_in_src,
                                self.counter.record_broken_attributes,
                                self.counter.record_indentical])   
                

    def create_part_file_errors_report(self, etl_part: InitParts, src_part: InitParts):
        if etl_part.compare_list or src_part.compare_list:
            self._create_non_matching_file_records(etl_part, src_part)
        #print(self.counter.errors[etl_part.name_part].keys())
        for num_field, errors in self.counter.errors[etl_part.name_part].items():
            name_field = f'_{etl_part.name_fields[num_field]}' if etl_part.name_fields else ''
            with open(join(self.path_folder, f'{etl_part.name_part}_num_fields_{num_field}_{name_field}.report'), 'a+') as f:
                f.write('\nDiff in file:\n {}\n '.format(self.filename))
                for value in errors:
                    f.write('*'*100)
                    f.write('\n    num_rec_etl: {} , num_rec_src: {} \n'.format(value[0][0], value[1][0]))
                    f.write('\n    ETL_FIELD_VALUE:{} , SRC_FIELD_VALUE: {} \n\n'.format(value[0][1][num_field], value[1][1][num_field]))
                    f.write('      ETL:{} \n      SRC:{} \n'.format(value[0][1], value[1][1]))
                    f.write('*'*100 + '\n\n')


    def _create_non_matching_file_records(self, etl_part: InitParts, src_part: InitParts):
        with open(join(self.path_folder, f'{etl_part.name_part}_non_matching_records.txt'), 'a+') as f:
            f.write('*'*100)
            f.write('\nNon-matching records in files:\n {}\n '.format(self.filename))
            f.write('\netl_len_records:{}\nrecord_only_in_etl:{}\n'.format(etl_part.len_records, len(etl_part.compare_list)))
            f.write('etl_record_repeated:{}\n'.format(self.counter.record_repeated_in_etl))
            for only_etl in etl_part.compare_list:
                f.write('    num_record_etl - {} : {}\n'.format(only_etl[0], only_etl[1]))
            f.write('\nsrc_len_records:{}\nrecord_only_in_src:{}\n'.format(src_part.len_records, len(src_part.compare_list)))
            f.write('src_record_repeated:{}\n'.format(self.counter.record_repeated_in_src))
            for only_src in src_part.compare_list:
                f.write('    num_record_src - {} : {}\n'.format(only_src[0], only_src[1]))
            f.write('*'*100 + '\n\n')


    @classmethod
    def create_error_file(cls, filename_error, filename):
        with open(join(cls.path_folder, '{}'.format(filename_error)), 'a+') as f:
            f.write("{}\n".format(filename))


    @classmethod
    def write_total_record(cls):
        with open(join(cls.path_folder, cls.result_path), 'a+') as f: 
            csv_writer = csv.writer(f, delimiter= ';')
            f.write(cls.comparison_header)
            csv_writer.writerow([
                                 'TOTAL', 
                                datetime.now(),
                                Counter.total_dict['record_len_etl'],
                                Counter.total_dict['record_len_src'],
                                Counter.total_dict['record_matched'],
                                Counter.total_dict['record_only_in_etl'],
                                Counter.total_dict['record_only_in_src'],
                                Counter.total_dict['record_repeated_in_etl'],
                                Counter.total_dict['record_repeated_in_src'],
                                Counter.total_dict['record_broken_attributes'],
                                Counter.total_dict['record_indentical'],
                                '{0:.2f}%'.format((Counter.total_dict['record_indentical']/Counter.total_dict['record_len_etl'])*100),
                                '{0:.2f}%'.format((Counter.total_dict['record_broken_attributes']/Counter.total_dict['record_len_etl'])*100)])
            print('TOTAL:')
            print('\n'.join(['{}: {}'.format(attr,str(value)) for attr, value in Counter.total_dict.items() if attr.startswith('record_')]) )
            csv_writer.writerow(['Field', 'Number Errors'])
            for key, value in Counter.total_dict.items():
                if not key.startswith('record_') and value != 0:
                    csv_writer.writerow([key, value])


class Logger:
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)
    stream_handler = StreamHandler(stream=sys.stdout)
    stream_formatter = logging.Formatter('%(levelname)s. %(message)s')
    file_formatter = logging.Formatter('%(asctime)s: %(levelname)s. %(message)s')
    stream_handler.setFormatter(file_formatter)
    logger.addHandler(stream_handler)
    if setting.LOG_IN_FILE:
        file_handler = logging.handlers.RotatingFileHandler(
            "{}_{}.log".format(join(ReportAll.path_folder, setting.NAME_OUTPUT), ReportAll.date),
            mode='a',
            maxBytes=setting.LOG_FILE_MAX_SIZE * 1024 * 1024,
            backupCount=setting.LOG_FILE_BACKUP_COUNT)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)


def main(list_files):
    for filename in list_files:
        etlReader = FileReader(filename[1]['etl'], etalon.prepare, etalon.encode)
        srcReader = FileReader(filename[1]['src'], source.prepare, source.encode)
        if filename[1]['etl'] and filename[1]['src']:
            counter = Counter()
            etl = PartsFactory(etlReader, etalon.new_path, filename[0])
            src = PartsFactory(srcReader, source.new_path, filename[0])
            differentRecords = RecordSeparation(etl, src, counter)
            differentRecords.difference_types()
            logger_stat(filename[0], counter)
            comparer = PartsComparison(etl, src, counter)
            comparer.execute()
            reportAll = ReportAll(etl, src, counter, filename[0])
            if counter.errors:
                reportAll.create_file_errors_report()
            reportAll.write_file_report_csv()
            logger_main(filename[0], counter)
        else:
            error_file = 'Etalon' if not filename[1]['etl'] else 'Source'
            Logger.logger.error("Missing records {} for the file: {}. ".format(error_file, filename[0]))
            ReportAll.create_error_file('{} missing files'.format(error_file) , filename[0])
    return Counter.total_dict

def logger_stat(filename, counter):
        Logger.logger.info(f"Comparing file: {filename}")
        Logger.logger.info(f"Line count ETL: {counter.record_len_etl}. Line count SRC: {counter.record_len_src}")       

def logger_main(filename:str, counter: Counter):
        #Logger.logger.info(f"Comparing file: {filename}")
        #Logger.logger.info(f"Line count ETL: {counter.record_len_etl}. Line count SRC: {counter.record_len_src}")
        Logger.logger.info(f"Matched keys: {counter.record_matched}. Only in PiOne: {counter.record_only_in_src}. Only in Etalon: {counter.record_only_in_etl}")
        Logger.logger.info(f"Broken attributes: {counter.record_broken_attributes}")

def compare_multithreading(list_files):
    n_workers = 10
    chunksize = round(len(list_files) / n_workers)
    with ThreadPoolExecutor(n_workers) as exe:
        # split the move operations into chunks
        for i in range(0, len(list_files), chunksize):
            # select a chunk of filenames
            filenames = list_files[i:(i + chunksize)]
            results = exe.submit(main, filenames)
            #print(results.result())


def compare_multipocessing(list_files):
    n_workers = multiprocessing.cpu_count()
    chunksize = round(len(list_files) / n_workers) + 1
    filenames = []
    with multiprocessing.Pool(n_workers) as pool:
        for i in range(0, len(list_files), chunksize):
            filenames.append(list_files[i:(i + chunksize)])
        result = pool.map(main, filenames)
    for keys in Counter.total_dict.keys():
        for res in result:
            Counter.total_dict[keys] += res[keys]
    return result


if __name__ == '__main__':
    etalon = Etalon(setting.ETL)
    source = Source(setting.SRC)
    files = FilesFactory()
    collect = files.create_collect(etalon, source)
    collect.get_files()
    list_files = list(File.names_files.items())
    start = time.time() 
    if list_files:   
        for attr_name in setting.BODY_NAMES:
            Counter.total_dict[attr_name] = 0
        if len(list_files) > 500:
            compare_multipocessing(list_files)
        elif len(list_files) > 100:
            compare_multithreading(list_files)
        else:
            main(list_files)
        ReportAll.write_total_record()
    print(time.time() - start)

        

