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


class FilesCollection:


    def __init__(self, etl_path: str, src_path :str) -> None:
        self.etl = etl_path
        self.src = src_path
        self.file_list_compare = []
        self.dict_compare = {}
        self.dict_compare['etl'] : list = []
        self.dict_compare['src'] : list = []
        

    def get_list_files(self) -> list: 
        if isdir(self.etl) and isdir(self.src):
            self.path_etl = os.path.abspath(self.etl)
            self.path_src = os.path.abspath(self.src)
            for file_etl, file_src in zip(os.listdir(self.path_etl), os.listdir(self.path_src)):
                self._prepare_name(file_etl, file_src)
                #print(file_etl)
                #self.dict_compare['etl'].append(file_etl) 
                #self.dict_compare['src'].append(file_src)
        elif isfile(self.etl) and isfile(self.src):
            self.path_etl, self.etl = os.path.split(os.path.abspath(self.etl))
            self.path_src, self.src = os.path.split(os.path.abspath(self.src))
            self._prepare_name(self.etl, self.src)
            # self.dict_compare['etl'].append(self.etl) 
            # self.dict_compare['src'].append(self.src)
        else:
            print("Сравнить можно либо папки, либо файлы")
        return self.file_list_compare
    

    def _prepare_name(self, etl_name, src_name):
        if etl_name == src_name:
            self.file_list_compare.append(etl_name)
        else:
            self.dict_compare[etl_name]
            print("Нужно выражения для обработки названия файлов")


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

    def __init__(self, records) -> None:
        self.len_records = len(records)
        self.records = records
        
        self.set_records = set(self.records)
        self.len_set_records = len(self.set_records)
        self.create_parts_document()

    def _prepare_etalon(self, etl, src):
        self.etl = etl
        self.src = src
        if self.prepare_etl:
            self.etl = etl[self.num_record_header:-self.num_record_trailer]
    
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
                    self.counter.set_attr('record_broken_attributes', 1)
                    self.counter.set_attr('record_matched', 1)
                    self.counter.errors[etl_part.name_part][diff_field[0]].append((etl_rec, src_rec))
        


class ReportAll:
    date = datetime.now().date()

    comparison_header = 'Comparing file;Date;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Records repeat in etl;Records repeat in src;Broken attributes same ID;Identical\n'
    if not exists(setting.RES):
        os.mkdir(setting.RES)
    path_folder = join(setting.RES, f'result_{str(datetime.now().date())}')
    #if not exists(path_folder):
    os.mkdir(path_folder)
    with open(join(path_folder, f'result_{date}.csv'), 'w+')  as csv_file: 
        csv_file.write(comparison_header)


    def __init__(self, etl: RecordPrepare, src: RecordPrepare, counter: Counter, filename: str) -> None:
        self.etl = etl
        self.src = src
        self.counter = counter
        self.filename = filename
        
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
        with open(join(self.path_folder, f'result_{self.date}.csv'), 'a+') as csv_file: 
            csv_writer = csv.writer(csv_file, delimiter = ';')
            csv_writer.writerow([self.filename, 
                                    datetime.now(),
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
        all_error_text = ''
        #rrors = self.counter['errors']
        #for num_field, errors in self.counter.errors.items():
        #print(self.counter.errors[etl_part.name_part].keys())
        for num_field, errors in self.counter.errors[etl_part.name_part].items():
            name_field = f'_{etl_part.name_fields[num_field]}' if etl_part.name_fields[0] != "" else ''
            errors_text = ''
            with open(join(self.path_folder, f'{etl_part.name_part}_num_fields_{num_field}_{name_field}.report'), 'a+') as report_file:
                report_file.write('\nDiff in file:{}\nrecord_only_in_etl:{}\nrecord_only_in_src:{}\n'.format(self.filename, len(etl_part.compare_list), len(src_part.compare_list)))
                report_file.write('etl_len_records:{}\nsrc_len_records:{}\n\n'.format(etl_part.len_records, src_part.len_records))
                print()
                for value in errors:
                    report_file.write('*'*40)
                    report_file.write('\n    num_rec_etl: {} , num_rec_src: {} \n'.format(value[0][0], value[1][0]))
                    report_file.write('\n    ETL_FIELD_VALUE:{} , SRC_FIELD_VALUE: {} \n\n'.format(value[0][1][num_field], value[1][1][num_field]))
                    report_file.write('      ETL:{} \n      SRC:{} \n'.format(value[0][1], value[1][1]))
                    report_file.write('*'*40+ '\n\n')
                    #report_file.write(errors_text)
                    #all_error_text+=errors_text
        return all_error_text

    
    @classmethod
    def write_total_record(cls):
        with open(join(cls.path_folder, f'result_{cls.date}.csv'), 'a+') as csv_file: 
            csv_writer = csv.writer(csv_file, delimiter= ';')
            csv_writer.writerow(['TOTAL', 
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
        counter = Counter()
        etlReader = FileReader(files.path_etl, filename)
        srcReader = FileReader(files.path_src, filename)
        etl = RecordPrepare(etlReader.records)
        src = RecordPrepare(srcReader.records)
        differentRecords = RecordSeparation(etl, src, counter)
        differentRecords.difference_types()
        comparer = PartsComparison(etl, src, counter)
        comparer.execute()
        reportAll = ReportAll(etl, src, counter, filename)
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
    # create the process pool
    with ThreadPoolExecutor(n_workers) as exe:
        #results = list(exe.map(move_files, files))
        # split the move operations into chunks
        for i in range(0, len(list_files), chunksize):
            #print(i)
            # select a chunk of filenames
            filenames = list_files[i:(i + chunksize)]
            results = exe.submit(main, filenames)
            print(results.result())

def compare_multipocessing(list_files):
    n_workers = multiprocessing.cpu_count()
    chunksize = round(len(list_files) / n_workers) + 1
    result = []
    filenames = []
    with multiprocessing.Pool(n_workers) as pool:
        for i in range(0, len(list_files), chunksize):
            filenames.append(list_files[i:(i + chunksize)])
        result = pool.map(main, filenames)
    for keys in Counter.total_dict.keys():
        for res in result:
            Counter.total_dict[keys] += res[keys]
    return result


    
files = FilesCollection(setting.ETL, setting.SRC)
list_files = files.get_list_files()


if __name__ == '__main__':
    start = time.time()    
    if len(list_files) > 1000:
        compare_multipocessing(files.path_etl, files.path_src, list_files)
    elif len(list_files) > 100:
        compare_multithreading(files.path_etl, files.path_src, list_files)
    else:
        main(files.path_etl, files.path_src, list_files)
    ReportAll.write_total_record()
    print(time.time() - start)

        

