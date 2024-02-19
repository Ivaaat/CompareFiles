import setting
import csv
from datetime import datetime
import codecs
from os.path import join, isfile, isdir, exists, basename
from concurrent.futures import ThreadPoolExecutor
import os
from collections import defaultdict
import logging
import multiprocessing
import time
from abc import ABC, abstractmethod
import re
import shutil


class FilesCollection:


    def __init__(self, etl_path: str, src_path :str) -> None:
        self.etl = etl_path
        self.src = src_path
        self.file_list_compare = []
        self.dict_compare = {}
        self.i = 1
        self.counter_error_rename = 0
        self.names_files = {}
        

    def get_list_files(self) -> list: 
        if isdir(self.etl) and isdir(self.src):
            self.path_etl = os.path.abspath(self.etl)
            self.path_src = os.path.abspath(self.src)
            etalon_files = os.listdir(self.path_etl)
            source_files = os.listdir(self.path_src)
            etalon_files.sort()
            source_files.sort()
            #for file_etl, file_src in zip(etalon_files, source_files):
            for file_etl in etalon_files:
                etl_name = re.sub(setting.REGEX_RENAME, '.cdr', file_etl)
                if etl_name not in self.names_files:
                    self.names_files[etl_name] = {'etl': [], 'src': []}
                self.names_files[etl_name]['etl'].append(file_etl)
                for file_src in source_files:
                    src_name = re.sub(setting.REGEX_RENAME, '.cdr', file_src)
                    if etl_name == src_name:
                        self.names_files[etl_name]['src'].append(file_src)
                        source_files.remove(file_src)
                    else:
                        break
                # self.counter_error_rename = 0
                # self._prepare_name(file_etl, file_src)
        elif isfile(self.etl) and isfile(self.src):
            self.path_etl, self.etl = os.path.split(os.path.abspath(self.etl))
            self.path_src, self.src = os.path.split(os.path.abspath(self.src))
            self._prepare_name(self.etl, self.src)
        else:
            print("Сравнить можно либо папки, либо файлы, проверь пути в config_comparison.ini")
        return list(self.names_files.items())
    

    def _prepare_name(self, etl_name, src_name, rename = False):

        if etl_name == src_name:
            self.file_list_compare.append(etl_name)
            if rename:
                os.rename(join(self.path_etl, self.etl_name), join(self.path_etl, etl_name))
                os.rename(join(self.path_src, self.src_name), join(self.path_src, src_name))
                self.i+=1
        else:
            if self.counter_error_rename > 0:
                return print('файлы не равны regex не сработал')
            self.etl_name = etl_name
            self.src_name = src_name
            etl_name = re.sub(setting.REGEX_RENAME, '_{}.cdr'.format(str(self.i)), etl_name)
            src_name = re.sub(setting.REGEX_RENAME, '_{}.cdr'.format(str(self.i)), src_name)   
            self.names_files[etl_name] = src_name if etl_name == src_name else ""

            self.counter_error_rename +=1
            self._prepare_name(etl_name, src_name, rename = True)


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

    def __init__(self, path, filename):
        with codecs.open(join(path, filename), 'r', encoding='UTF-8') as f:
            self.records = f.readlines()


class RecordPrepare:
    
    full_compare = False

    def __init__(self, records, prepare) -> None:
        self.records = records if not prepare else records[setting.NUM_REC_HEADER:-setting.NUM_REC_TRAILER]
        self.len_records = len(self.records)
        self.set_records = set(self.records)
        self.len_set_records = len(self.set_records)
        self.create_parts_document()

    
    
    def create_parts_document(self):
        if setting.HEADER_SIZE and setting.BODY_SIZE and setting.TRAILER_SIZE:
            self.header = Header(self.records[:setting.NUM_REC_HEADER], setting.NUM_REC_HEADER)
            self.body = Body(self.records[setting.NUM_REC_HEADER:-setting.NUM_REC_TRAILER], setting.NUM_REC_HEADER + 1)
            self.trailer = Trailer(self.records[-setting.NUM_REC_TRAILER:], self.len_records - setting.NUM_REC_TRAILER + 1)
            RecordPrepare.full_compare = True
        elif setting.BODY_SIZE:
            self.body = Body(self.records, 1)


            


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
    

    def _clear_split_record(self):
        if setting.TYPE_DELIMITER == 'char':
            self.__clear_split_record_delimeter()
        elif setting.TYPE_DELIMITER == 'fields':
            self.__clear_split_record_fields()


    def __clear_split_record_delimeter(self):
        self.compare_list = []
        for record in self.diff_res:
            split_record = record.split(self.delimiter)
            records_list = []
            for field in split_record:
                records_list.append(field.strip())
            self.compare_list.append((self.num_record_dict[record],records_list))
        self.compare_list
    

    def __clear_split_record_fields(self):
        self.compare_list = []
        for record in self.diff_res:
            records_list = []
            start = 0
            for size_field in self.size_fields:
                records_list.append(record[start:start + size_field].strip())
                start+=size_field
            self.compare_list.append((self.num_record_dict[record], records_list))
        self.compare_list
        
        
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

    def __init__(self, etl: RecordPrepare, src: RecordPrepare, counter: Counter) -> None:
        self.etl = etl
        self.src = src
        self.counter = counter
        self.counter.set_attr('record_len_etl', etl.len_records)
        self.counter.set_attr('record_len_src', src.len_records)
        self.intersection = {}
        

    def difference_types(self):
        if RecordPrepare.full_compare:
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
        etl_part.diff_res = etl_part.num_record_dict.keys() - src_part.num_record_dict.keys() 
        src_part.diff_res = src_part.num_record_dict.keys() - etl_part.num_record_dict.keys() 
        self.intersection = etl_part.num_record_dict.keys() & src_part.num_record_dict.keys()
        self.counter.set_attr('record_matched', len(self.intersection))
        self.counter.set_attr('record_indentical', len(self.intersection))
        self.counter.set_attr('record_only_in_etl', len(etl_part.diff_res))
        self.counter.set_attr('record_only_in_src', len(src_part.diff_res))
        #self._check_repeat_records(etl_part, src_part)
        etl_part._clear_split_record()
        src_part._clear_split_record()


class PartsComparison:
    def __init__(self, etl: RecordPrepare, src: RecordPrepare, counter: Counter):
        self.etl = etl
        self.src = src
        self.counter = counter


    def execute(self):
        if RecordPrepare.full_compare:
            self._compare_fields(self.etl.header, self.src.header)
            self._compare_fields(self.etl.body, self.src.body)
            self._compare_fields(self.etl.trailer, self.src.trailer)
        else:
            self._compare_fields(self.etl.body, self.src.body)

    def _compare_fields(self, etl_part: InitParts, src_part: InitParts):
        self.counter.errors[etl_part.name_part] = defaultdict(list)
        if len(src_part.compare_list) > setting.MAX_LOG_FILE:
            return
        for src_rec in  src_part.compare_list:
            for etl_rec in etl_part.compare_list:
                diff_field = []
                for i in range(len(etl_part.size_fields)):
                    try:
                        if src_rec[1][i] != etl_rec[1][i]:
                            diff_field.append(i)
                    except IndexError:
                        break
                if len(diff_field) == 1:
                    if len(self.counter.errors[etl_part.name_part][diff_field[0]]) > setting.MAX_LOG_FILE:
                        return
                    src_rec.compare_list.remove(etl_rec)
                    etl_part.compare_list.remove(etl_rec)
                    self.counter.set_attr('record_broken_attributes', 1)
                    self.counter.set_attr('record_matched', 1)
                    self.counter.set_attr('record_only_in_etl', -1)
                    self.counter.set_attr('record_only_in_src', -1)
                    self.counter.errors[etl_part.name_part][diff_field[0]].append((etl_rec, src_rec))
                    try:
                        name_field = '{}'.format(etl_part.name_fields[diff_field[0]])
                    except IndexError:
                        name_field = '{}'.format(diff_field[0])
                    Counter.total_dict[name_field]+=1
                    break

        


class ReportAll:
    date = datetime.now().strftime("%d-%m-%y_%H%M%S")

    comparison_header = 'Comparing file Etalon;Comparing file Source;Date;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Records repeat in etl;Records repeat in src;Broken attributes same ID;Identical\n'
    result_path = '{}_{}_Comparison_Result.csv'.format(setting.NAME_OUTPUT, date)
    if not exists(setting.RES):
        os.mkdir(setting.RES)
    path_folder = join(setting.RES, '{}_diff_{}'.format(setting.NAME_OUTPUT, date))
    if not exists(path_folder):
        os.mkdir(path_folder)
    with open(join(path_folder, result_path), 'w+')  as csv_file: 
        csv_file.write(comparison_header)


    def __init__(self, etl: RecordPrepare, src: RecordPrepare, counter: Counter, filename_etl: str, filename_src: str) -> None:
        self.etl = etl
        self.src = src
        self.counter = counter
        self.filename_etl = filename_etl
        self.filename_src = filename_src
        
    def create_file_errors_report(self):
        error_text = ''
        if RecordPrepare.full_compare:
            error_text+=self.create_part_file_errors_report(self.etl.header, self.src.header)
            error_text+=self.create_part_file_errors_report(self.etl.body, self.src.body)
            error_text+=self.create_part_file_errors_report(self.etl.trailer, self.src.trailer)
        else:
            error_text+= self.create_part_file_errors_report(self.etl.body, self.src.body)
        return error_text
        
    
    
    def write_file_report_csv(self):
        with open(join(self.path_folder, self.result_path), 'a+') as csv_file: 
            csv_writer = csv.writer(csv_file, delimiter = ';')
            csv_writer.writerow([self.filename_etl, 
                                 self.filename_src,
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
                

    def create_part_file_errors_report(self, etl_part, src_part):
        if etl_part.compare_list or src_part.compare_list:
            self._create_non_matching_file_records(etl_part, src_part)
        all_error_text = ''
        for num_field, errors in self.counter.errors[etl_part.name_part].items():
            name_field = f'_{etl_part.name_fields[num_field]}' if etl_part.name_fields else ''
            errors_text = ''
            with open(join(self.path_folder, f'{etl_part.name_part}_num_fields_{num_field}_{name_field}.report'), 'a+') as report_file:
                report_file.write('\nDiff in file:\n ETL:{}\n SRC:{}\n'.format(self.filename_etl,self.filename_src))
                for value in errors:
                    report_file.write('*'*100)
                    report_file.write('\n    num_rec_etl: {} , num_rec_src: {} \n'.format(value[0][0], value[1][0]))
                    report_file.write('\n    ETL_FIELD_VALUE:{} , SRC_FIELD_VALUE: {} \n\n'.format(value[0][1][num_field], value[1][1][num_field]))
                    report_file.write('      ETL:{} \n      SRC:{} \n'.format(value[0][1], value[1][1]))
                    report_file.write('*'*100 + '\n\n')
                    #report_file.write(errors_text)
                    #all_error_text+=errors_text
        return all_error_text


    def _create_non_matching_file_records(self, etl_part, src_part):
        with open(join(self.path_folder, f'{etl_part.name_part}_non_matching_records.txt'), 'a+') as non_matching_records:
            non_matching_records.write('*'*100)
            non_matching_records.write('\nNon-matching records in files:\n ETL:{}\n SRC:{}\n'.format(self.filename_etl, self.filename_src))
            non_matching_records.write('\netl_len_records:{}\nrecord_only_in_etl:{}\n'.format(etl_part.len_records, len(etl_part.compare_list)))
            for only_etl in etl_part.compare_list:
                non_matching_records.write('    num_record_etl - {} : {}\n'.format(only_etl[0], only_etl[1]))
            non_matching_records.write('\nsrc_len_records:{}\nrecord_only_in_src:{}\n'.format(src_part.len_records, len(src_part.compare_list)))
            for only_src in src_part.compare_list:
                non_matching_records.write('    num_record_src - {} : {}\n'.format(only_src[0], only_src[1]))
            non_matching_records.write('*'*100 + '\n\n')

    
    @classmethod
    def write_total_record(cls):
        with open(join(cls.path_folder, cls.result_path), 'a+') as csv_file: 
            csv_writer = csv.writer(csv_file, delimiter= ';')
            csv_file.write(cls.comparison_header)
            csv_writer.writerow(['',
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
            csv_writer.writerow(['Field', 'Number Errors'])
            for key, value in Counter.total_dict.items():
                if not key.startswith('record_') and value != 0:
                    csv_writer.writerow([key, value])

            

class CompareCommand:
    def __init__(self, etl: RecordPrepare, src: RecordPrepare, filename: str):
        self.etl = etl
        self.src = src
        self.filename = filename
    
    def compare_fields(self, lines1, lines2, cav_file):
        total_lines = len(lines1)
        lines_match = 0
        fields_match = 0
        lines_not_match = 0
        fields_not_match = 0
   

def main(list_files):
    for filename in list_files:
        if filename[1]['etl'] != filename[1]['src'] and filename[0].endswith('.cdr'):
            filename[1]['etl'].sort(key=lambda x : int(x.split('_')[-1][:-4]))
            filename[1]['src'].sort(key=lambda x : int(x.split('_')[-1][:-4]))
        for etl_filename, src_filename in zip(filename[1]['etl'], filename[1]['src']):
            counter = Counter()
            etlReader = FileReader(files.path_etl, etl_filename)
            srcReader = FileReader(files.path_src, src_filename)
            etl = RecordPrepare(etlReader.records, setting.PREPARE_ETL)
            src = RecordPrepare(srcReader.records, setting.PREPARE_SRC)
            differentRecords = RecordSeparation(etl, src, counter)
            differentRecords.difference_types()
            comparer = PartsComparison(etl, src, counter)
            comparer.execute()
            reportAll = ReportAll(etl, src, counter, etl_filename, src_filename)
            if counter.errors:
                error_text = reportAll.create_file_errors_report()
            reportAll.write_file_report_csv()
        #logger = logging.getLogger('logger')
        #logger.setLevel(logging.DEBUG)
        #logger.warning(error_text)

    return Counter.total_dict
       

def compare_multithreading(list_files):
    n_workers = 10
    chunksize = round(len(list_files) / n_workers)
    with ThreadPoolExecutor(n_workers) as exe:
        # split the move operations into chunks
        for i in range(0, len(list_files), chunksize):
            # select a chunk of filenames
            filenames = list_files[i:(i + chunksize)]
            results = exe.submit(main, filenames)
            print(results.result())


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
    files = FilesCollection(setting.ETL, setting.SRC)
    list_files = files.get_list_files()
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

        

