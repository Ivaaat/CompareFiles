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
from typing import List, Union






class Files:

    names_files = set()
    rename = False
    
    def __init__(self) -> None:
        self.path: str
        self.mask_files: str
        self.isdir = isdir(self.path)
        self.isfile = isfile(self.path)
        self.regex = setting.REGEX_RENAME
        self.new_path = ''
        self.files = {}
        

    def get_files_dir(self):
        self.path = os.path.abspath(self.path)
        self.path_filenames = glob.glob(join(self.path, self.mask_files))
        self.filenames = [os.path.split(file)[1] for file in self.path_filenames]
    

    def сheck_and_rename_files(self):
        Logger.logger.info(f"Check name files {str(self)}")
        for i, file in enumerate(self.path_filenames, 1):
            filename = os.path.split(file)[1]
            if isfile(file):
                Logger.logger.info(f'Check {i} = {file}')
                if Files.rename:
                    try:
                        file_name = ''.join(re.findall(self.regex, filename)[0])
                    except IndexError:
                        Logger.logger.error('Regexp not match with the file {}.'.format(filename))
                        ReportAll.create_error_file('Regexp not match in {}'.format(str(self)) , filename)
                        print(f'Error regex in {file}')
                        continue
                else:
                    file_name = os.path.split(file)[1]
                if file_name not in self.files:
                    self.files[file_name] =  []
                self.files[file_name].append(file)
                Files.names_files.add(file_name)
                
                


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


class EtalonFiles(Files):
    
    def __init__(self) -> None:
        self.path = setting.ETL
        self.mask_files = setting.MASK_FILES_ETALON
        super().__init__()

    
    def __str__(self) -> str:
        return 'etl'



class SourceFiles(Files):
    
    def __init__(self) -> None:
        self.path = setting.SRC
        self.mask_files = setting.MASK_FILES_ETALON
        super().__init__()
    
    
    def __str__(self) -> str:
        return 'src'

    

class File:

    def __init__(self) -> None:
        self.new_path: str
        self.records: List[str] = []
        self.num_records = 0
        self.unique_records = 0
        self.num_unique_records = 0
        self.header: Header
        self.body: Body
        self.trailer: Trailer
        self.files: List[str] = []
   


class Etalon(File):
    
    def __init__(self, etalon_files: EtalonFiles) -> None:
        super().__init__()
        self.prepare = setting.PREPARE_ETL
        self.encode = setting.ENCODE_FILES_ETALON
        self.files = etalon_files.files
        self.new_path = etalon_files.new_path


class Source(File):

    
    def __init__(self, source_files: EtalonFiles) -> None:
        super().__init__()
        self.prepare = setting.PREPARE_SRC
        self.encode = setting.ENCODE_FILES_SOURCE
        self.files = source_files.files
        self.new_path = source_files.new_path
    

class FileCompare:

    def __init__(self, etalon: Etalon, source: Source, filename: str) -> None:
        self.etalon = etalon
        self.source = source
        self.filename = filename
        etalon.files[filename]
        source.files[filename]

class Collect:
    def __init__(self, etalon: Files, source: Files) -> None:
        self.etalon = etalon
        self.source = source
    

class CollectFiles(Collect):

    def __init__(self, etalon: EtalonFiles, source: SourceFiles) -> None:
        super().__init__(etalon, source)
    

    def get_files(self):
        self.etalon.get_files_dir()
        self.source.get_files_dir()
        if (set(self.etalon.filenames) ^ set(self.source.filenames)):
            Files.rename = True 
            self.etalon._create_new_folder()
            self.source._create_new_folder()
        self.etalon.сheck_and_rename_files()
        self.source.сheck_and_rename_files()
    

class CollectFile(Collect):

    def __init__(self, etalon: EtalonFiles, source: SourceFiles) -> None:
        super().__init__(etalon, source)


    def get_files(self):
        filename_etl = os.path.split(self.etalon.path)[1]
        filename_src = os.path.split(self.source.path)[1]
        file_name = '{} | {}'.format(filename_etl, filename_src)
        self.etalon.files[file_name] = [self.etalon.path]
        self.source.files[file_name] = [self.source.path]
        Files.names_files.add(file_name)
        
        

class FilesFactory:

    def __init__(self, etalon: EtalonFiles, source: SourceFiles) -> None:
        if etalon.isdir and source.isdir:
            self.collector = CollectFiles(etalon, source)
        elif etalon.isfile and source.isfile:
            self.collector = CollectFile(etalon, source)
        else:
            print("Сравнить можно либо папки, либо файлы, проверь пути в config_comparison.ini")
            sys.exit()


class Counter:
    
    total_dict = {
                'record_len_etl': 0, 
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
        if buff:
            new_val = getattr(self, attr) + buff
            setattr(self, attr, new_val)
            Counter.total_dict[attr] += buff
    

class FileReader:

    def __init__(self, file: FileCompare):
        self.filename = file.filename
        self.read(file.etalon)
        self.read(file.source)
        
    
    def read(self, file:  Union[Etalon, Source]):
        for path_file in file.files[self.filename]:
            with codecs.open(path_file, 'r', encoding = file.encode) as f:
                file.records += f.readlines() if not file.prepare else f.readlines()[setting.NUM_REC_HEADER:setting.NUM_REC_TRAILER]
        file.num_records = len(file.records)
        file.unique_records = set(file.records)
        file.num_unique_records = len(file.unique_records)


class FileWriter:
    filename_write = ''

    @staticmethod
    def write(file: FileCompare):
        FileWriter.filename_write = file.filename
        FileWriter._write(file.etalon)
        FileWriter._write(file.source)
        

    @staticmethod
    def _write(file: Union[Etalon, Source]):
        with open(join(file.new_path, FileWriter.filename_write), "w+", encoding='UTF-8') as f:
            for rec in file.records:
                f.write(rec.strip() + '\n')

                

class PartsFactory:
    
    full_compare = False


    def __init__(self, file: FileCompare) -> None:
        self._create_parts_document(file.etalon)
        self._create_parts_document(file.source)
  

    def _create_parts_document(self, file: Union[Etalon, Source]):
        if setting.HEADER_SIZE and setting.BODY_SIZE and setting.TRAILER_SIZE:
            file.header = Header(file.records[:setting.NUM_REC_HEADER], setting.NUM_REC_HEADER)
            file.body = Body(file.records[setting.NUM_REC_HEADER:-setting.NUM_REC_TRAILER], setting.NUM_REC_HEADER + 1)
            file.trailer = Trailer(file.records[-setting.NUM_REC_TRAILER:], file.num_records - setting.NUM_REC_TRAILER + 1)
            PartsFactory.full_compare = True
        else:
            file.body = Body(file.records, 1)


class InitParts:
    def __init__(self, records : List[str], start : int) -> None:
        self.start = start
        self.records: List[str] = []
        self.len_records = len(records)
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
        self.set_records = set(records)
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

    def __init__(self, file: FileCompare, counter: Counter) -> None:
        self.etl = file.etalon
        self.src = file.source
        self.counter = counter
        self.counter.set_attr('record_len_etl', self.etl.num_records)
        self.counter.set_attr('record_len_src', self.src.num_records)
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
    def __init__(self, file: FileCompare, counter: Counter):
        self.etl = file.etalon
        self.src = file.source
        self.counter = counter
        self.filename = file.filename


    def execute(self):
        if PartsFactory.full_compare:
            self._compare_fields(self.etl.header, self.src.header)
            self._compare_fields(self.etl.body, self.src.body)
            self._compare_fields(self.etl.trailer, self.src.trailer)
        else:
            self._compare_fields(self.etl.body, self.src.body)

    


    def _compare_fields(self, etl_part: Union[Header, Body, Trailer], src_part: Union[Header, Body, Trailer]):
        self.counter.errors[etl_part.name_part] = defaultdict(list)
        max_broken_list = []
        src_gen = (x for x in src_part.compare_list.copy())
        for src_rec in src_gen:
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
                            Logger.logger.info(f"{self.filename}. THE MAXIMUM VALUE OF ATTRIBUTES IS EXCEEDED \"MAX_BROKEN_ATTRIBUTES\" Name: {etl_part.name_fields[field]}  Num: {field}")
                        continue
                    self.counter.set_attr('record_broken_attributes', 1)
                    self.counter.errors[etl_part.name_part][field].append((broken_records[0][0], src_rec))
                    try:
                        name_field = '{}'.format(etl_part.name_fields[field])
                    except IndexError:
                        name_field = '{}'.format(field)
                    Counter.total_dict[name_field]+=1


        


class ReportAll:
    # date = datetime.now().strftime("%d-%m-%y_%H%M%S")
    # comparison_header = 'Comparing file;Date;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Records repeat in etl;Records repeat in src;Broken attributes same ID;Identical\n'
    # result_path = '{}_{}_Comparison_Result.csv'.format(setting.NAME_OUTPUT, date)
    # if not exists(setting.RES):
    #     os.mkdir(setting.RES)
    # path_folder = join(setting.RES, '{}_diff_{}'.format(setting.NAME_OUTPUT,  date))
    # if not exists(path_folder):
    #     os.mkdir(path_folder)
    # with open(join(path_folder, result_path), 'w+')  as csv_file: 
    #     csv_file.write(comparison_header)


    def __init__(self, file: FileCompare, counter: Counter) -> None:
        self.etl = file.etalon
        self.src = file.source
        self.counter = counter
        self.filename = file.filename

    @classmethod
    def create_folder_errors_report(cls):
        date = datetime.now().strftime("%d-%m-%y_%H%M%S")
        comparison_header = 'Comparing file;Date;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Records repeat in etl;Records repeat in src;Broken attributes same ID;Identical\n'
        result_path = '{}_{}_Comparison_Result.csv'.format(setting.NAME_OUTPUT, date)
        if not exists(setting.RES):
            os.mkdir(setting.RES)
        cls.path_folder = join(setting.RES, '{}_diff_{}'.format(setting.NAME_OUTPUT,  date))
        if not exists(cls.path_folder):
            os.mkdir(cls.path_folder)
        with open(join(cls.path_folder, result_path), 'w+')  as csv_file: 
            csv_file.write(comparison_header)


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
                                self.etl.num_records,
                                self.src.num_records,
                                self.counter.record_matched,
                                self.counter.record_only_in_etl,
                                self.counter.record_only_in_src,
                                self.counter.record_repeated_in_etl,
                                self.counter.record_repeated_in_src,
                                self.counter.record_broken_attributes,
                                self.counter.record_indentical])   
                

    def create_part_file_errors_report(self, etl_part: Union[Header, Body, Trailer], src_part: Union[Header, Body, Trailer]):
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


    def _create_non_matching_file_records(self, etl_part: Union[Header, Body, Trailer], src_part: Union[Header, Body, Trailer]):
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
                                '{0:.2f}%'.format((Counter.total_dict['record_broken_attributes']/Counter.total_dict['record_len_etl'] * len(setting.BODY_NAMES))*100)])
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


def logger_stat(filename, counter: Counter):
        Logger.logger.info(f"Comparing file: {filename}")
        Logger.logger.info(f"Line count ETL: {counter.record_len_etl}. Line count SRC: {counter.record_len_src}")       


def logger_main(counter: Counter):
        Logger.logger.info(f"Matched keys: {counter.record_matched}. Only in PiOne: {counter.record_only_in_src}. Only in Etalon: {counter.record_only_in_etl}")
        Logger.logger.info(f"Broken attributes: {counter.record_broken_attributes}")




class Compare(Files):


    def __init__(self, files: List[FileCompare]) -> None:
        self.files = files
        self.num_files = len(self.files)
        
    

    def compare(self):
        self._compare_multipocessing()
        #self._main(self.files)
        ReportAll.write_total_record()
      
    

    def _compare_multipocessing(self):
        for attr_name in setting.BODY_NAMES:
            Counter.total_dict[attr_name] = 0
        n_workers = multiprocessing.cpu_count()
        chunksize = round(self.num_files / n_workers) + 1
        filenames = []
        with multiprocessing.Pool(n_workers) as pool:
            for i in range(0, self.num_files, chunksize):
                filenames.append(self.files[i:(i + chunksize)])
            result = pool.map(self._main, filenames)
        for keys in Counter.total_dict.keys():
            for res in result:
                Counter.total_dict[keys] += res[keys]
        return result
    

    def _main(self, files: List[FileCompare]):
        for attr_name in setting.BODY_NAMES:
            Counter.total_dict[attr_name] = 0
        for file in files:
            counter = Counter()
            FileReader(file)
            PartsFactory(file)
            if Files.rename:
                FileWriter.write(file)
            RecordSeparation(file, counter).difference_types()
            logger_stat(file.filename, counter)
            comparer = PartsComparison(file, counter)
            comparer.execute()
            reportAll = ReportAll(file, counter)
            if counter.errors:
                reportAll.create_file_errors_report()
            reportAll.write_file_report_csv()
            logger_main(counter)
        return Counter.total_dict




class MergeFiles:

    def __init__(self, etalon_files: EtalonFiles, source_files: SourceFiles) -> None:
        self.etalon_files = etalon_files
        self.source_files = source_files
        self.files = []


    def merge(self):
        for filename in Files.names_files:
            try:
                etalon = Etalon(self.etalon_files)
                source = Source(self.source_files)
                file = FileCompare(etalon, source, filename)
                self.files.append(file)
            except KeyError:
                error_file = 'Etalon' if filename not in self.etalon_files.files else 'Source'
                Logger.logger.error("Missing records {} for the file: {}. ".format(error_file, filename))
                ReportAll.create_error_file('{} missing files'.format(error_file) , filename)


class Records:

    def __init__(self, etalon: Etalon, source: Source, filename:str) -> None:
        pass
        

def timer(func):
    def wrapper():
        start = time.time() 
        func()
        print(time.time() - start)
    return wrapper

class FacadeCompare:

    def __init__(self) -> None:
        self.etalon_files = EtalonFiles()
        self.source_files = SourceFiles()


    def run(self):
        collect = FilesFactory(self.etalon_files, self.source_files)
        collect.collector.get_files()
        ReportAll.create_folder_errors_report()
        merge_files = MergeFiles(self.etalon_files, self.source_files)
        merge_files.merge()
        compare = Compare(merge_files.files)
        compare.compare()


if __name__ == '__main__':
    facade = FacadeCompare()
    facade.run()

        

