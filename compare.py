import json
import setting
import csv
from datetime import datetime
import codecs
from os.path import join, isfile, isdir, exists, basename
from concurrent.futures import ThreadPoolExecutor
import os
import logging
from abc import ABC, abstractmethod



class Counter:
    # total_len_etl = 0
    # total_len_src = 0
    # total_matched = 0
    # total_only_in_etl = 0
    # total_only_in_src = 0
    # total_repeated_in_etl = 0
    # total_repeated_in_src = 0
    # total_broken_attributes = 0
    # total_indentical = 0
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
    
    

class FilesCollection:


    def __init__(self, etl_path: str, src_path :str) -> None:
        self.etl = etl_path
        self.src = src_path
        self.file_list_compare = []
        

    def get_list_files(self) -> list: 
        if isdir(self.etl) and isdir(self.src):
            self.path_etl = os.path.abspath(self.etl)
            self.path_src = os.path.abspath(self.src)
            for file_etl, file_src in zip(os.listdir(self.path_etl), os.listdir(self.path_src)):
                self._prepare_name(file_etl, file_src)
        elif isfile(self.etl) and isfile(self.src):
            self.path_etl, self.etl = os.path.split(os.path.abspath(self.etl))
            self.path_src, self.src = os.path.split(os.path.abspath(self.src))
            self._prepare_name(self.etl, self.src)
        else:
            print("Сравнить можно либо папки, либо файлы")
        return self.file_list_compare
    

    def _prepare_name(self, etl_name, src_name):
        if etl_name == src_name:
            self.file_list_compare.append(etl_name)
        else:
            print("Нужно выражения для обработки названия файлов")

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




    
class RecordSeparation:

    def __init__(self, etl: RecordPrepare, src: RecordPrepare, counter: Counter) -> None:
        self.etl = etl
        self.src = src
        self.counter = counter
        Counter.total_dict['record_len_etl'] += etl.len_records
        Counter.total_dict['record_len_src'] += src.len_records
        
    

    def difference_types(self):
        if RecordPrepare.full_compare:
            self._difference_part(self.etl.header, self.src.header)
            self._difference_part(self.etl.body, self.src.body)
            self._difference_part(self.etl.trailer, self.src.trailer)
        else:
            self._difference_part(self.etl.body, self.src.body)
    
    def _check_repeat_records(self, type_file, diff, counter1, counter2):
        if type_file.repeated_elements:
            for item in diff:
                counter1+=type_file.repeated_elements[item]
                counter2+=type_file.repeated_elements[item] - 1


    def _difference_part(self, etl_part, src_part):
        difference_etl = etl_part.set_records.difference(src_part.set_records) 
        difference_src = src_part.set_records.difference(etl_part.set_records) 
        if etl_part.repeated_elements:
            for item in difference_etl:
                try:
                    self.counter.record_repeated_in_etl+=etl_part.repeated_elements[item]
                    self.counter.record_only_in_etl+=etl_part.repeated_elements[item] - 1
                except KeyError:
                    continue
        if src_part.repeated_elements:
            for item in difference_src:
                try:
                    self.counter.record_repeated_in_src+=src_part.repeated_elements[item]
                    self.counter.record_only_in_src+=src_part.repeated_elements[item] - 1
                except KeyError:
                    continue
        intersection = etl_part.set_records & src_part.set_records
        intersection_repeat = etl_part.repeated_elements.keys() & src_part.repeated_elements.keys()
        if intersection_repeat:
            for item in intersection_repeat:
                try:
                    self.counter.record_matched+=etl_part.repeated_elements[item] - 1
                    self.counter.record_indentical+=etl_part.repeated_elements[item] - 1
                except KeyError:
                    continue
        etl_part.get_num_record_diff(difference_etl)
        src_part.get_num_record_diff(difference_src)
        self.counter.set_attr('record_matched', len(intersection))
        self.counter.set_attr('record_indentical', len(intersection))
        self.counter.set_attr('record_only_in_etl', len(difference_etl))
        self.counter.set_attr('record_only_in_src', len(difference_src))
        #self.counter.record_matched+=len(intersection)
        #self.counter.record_indentical+=len(intersection)
        #self.counter.record_only_in_etl+=len(difference_etl)
        #self.counter.record_only_in_src+=len(difference_src)
       
        
            


class InitParts:
    def __init__(self, records : list, start : int) -> None:
        self.start = start
        self.records = []
        self.len_records = len(records)
        ###strip records self.records не равно records_for_compare
        self.records_for_compare = []
        self.delimiter = setting.DELIMITER
        self.repeated_elements = {}
        for i, record in enumerate(records, start):
            record = record.strip()
            self.records.append(record)
            self.records_for_compare.append((i, record))
            if self.records.count(record) > 1:
                self.repeated_elements[record] = self.records.count(record)
        self.set_records = set(self.records)
        self.len_repeat_records =  self.len_records - len(self.set_records)
    

    def get_num_record_diff(self, diff):
        self.diff_res = []
        for rec_diff in diff:
            for record_for_compare in self.records_for_compare:
                if rec_diff == record_for_compare[1]:
                    self.diff_res.append((record_for_compare))
        if setting.TYPE_DELIMITER == 'char':
            self.__clear_split_record_delimeter()
        elif setting.TYPE_DELIMITER == 'fields':
            self.__clear_split_record_fields()


    def __clear_split_record_delimeter(self):
        self.compare_list = []
        for record in self.diff_res:
            split_record = record[1].split(self.delimiter)
            records_list = []
            for field in split_record:
                records_list.append(field.strip())
            self.compare_list.append((record[0],records_list))
        self.compare_list
    

    def __clear_split_record_fields(self):
        self.compare_list = []
        for record in self.diff_res:
            records_list = []
            start = 0
            for size_field in self.size_fields:
                records_list.append(record[1][start:start + size_field])
                start+=size_field
            self.compare_list.append((record[0], records_list))
        self.compare_list
        
        
class Header(InitParts):

    def __init__(self, records : list, start : int) -> None:
        super().__init__(records, start)
        self.size_fields = setting.HEADER_SIZE
        self.name_fields = setting.HEADER_NAMES


class Body(InitParts):


    def __init__(self, records : list, start : int) -> None:
        super().__init__(records, start)
        self.size_fields = setting.BODY_SIZE
        self.name_fields = setting.BODY_NAMES


class Trailer(InitParts):


    def __init__(self, records : list, start : int) -> None:
        super().__init__(records, start)
        self.size_fields = setting.TRAILER_SIZE
        self.name_fields = setting.TRAILER_NAMES

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

    def _compare_fields(self, etl_part, src_part):
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
                    self.counter.set_attr('record_only_in_etl', -1)
                    self.counter.set_attr('record_only_in_src', -1)
                    # self.counter.record_broken_attributes+=1
                    # self.counter.record_matched+=1
                    # self.counter.record_only_in_etl-=1
                    # self.counter.record_only_in_src-=1
                    if diff_field[0] not in self.counter.errors:
                        self.counter.errors[diff_field[0]] = []
                    self.counter.errors[diff_field[0]].append((etl_rec, src_rec)) 
        


class ReportAll:
    date = datetime.now().date()

    comparison_header = 'Comparing file;Date;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Records repeat in etl;Records repeat in src;Broken attributes same ID;Identical\n'
    if not exists(setting.RES):
        os.mkdir(setting.RES)
    path_folder = join(setting.RES, f'result_{str(datetime.now().date())}')
    if not exists(path_folder):
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
            # Counter.total_len_etl+=self.etl.len_records
            # Counter.total_len_src+=self.src.len_records
            # Counter.total_matched+=self.counter.record_matched
            # Counter.total_only_in_etl+=self.counter.record_only_in_etl
            # Counter.total_only_in_src+=self.counter.record_only_in_src
            # Counter.total_repeated_in_etl+=self.counter.record_repeated_in_etl
            # Counter.total_repeated_in_src+=self.counter.record_repeated_in_src
            # Counter.total_broken_attributes+=self.counter.record_broken_attributes
            # Counter.total_indentical+=self.counter.record_indentical
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
        for num_field, errors in self.counter.errors.items():
            name_field = f'_{etl_part.name_fields[num_field]}' if etl_part.name_fields[0] != "" else ''
            errors_text = ''
            with open(join(self.path_folder, f'num_fields_{num_field}{name_field}.report'), 'w+') as report_file:
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
        
    
    
    
    


        

class FilePrepareDel(RecordPrepare):
    def __init__(self, delimiter, num_records_header = 1, num_records_trailer = 2, 
                 full= False, prepare_etl = False) -> None:
        super().__init__(num_records_header, num_records_trailer, full, prepare_etl)
        self.delimiter = delimiter

    def prepare(self, etl, src, filename):
        self.len_etl = len(etl)
        self.len_src = len(src)
        src = [record.strip() for record in src]
        etl = [record.strip() for record in etl]
        etl = tuple(filter(lambda x: x.startswith('-1'), etl))
        src = tuple(filter(lambda x: x.startswith('-1'), src ))

        self.set_etl = set(etl)
        self.set_src = set(src)
        difference_etl = self.set_etl.difference(self.set_src) 
        difference_src = self.set_src.difference(self.set_etl) 
        #print(len(difference_etl), len(difference_src)) 
        self.src_for_compare = [(i, record) for i, record in enumerate(src, 1)]
        self.etl_for_compare = [(i, record) for i, record in enumerate(etl, 1)]
        self.body_diff_etl = self.get_num_record_diff(self.etl_for_compare, difference_etl)
        self.body_diff_src = self.get_num_record_diff(self.src_for_compare, difference_src)
        #print(len(self.body_diff_etl), len(self.body_diff_src)) 
        if len(self.body_diff_etl) == 0 and len(self.body_diff_src) == 0:
            return
        #print(len(self.set_etl), len(self.set_src))
        self.split_record_etl = self._clear_split_record(self.body_diff_etl)
        self.split_record_src = self._clear_split_record(self.body_diff_src)
        compare_dict= {}
        #compare_list = []
        for src_rec in  self.split_record_src:
            for etl_rec in self.split_record_etl:
                diff_field = []
                for i in range(len(setting.BODY_SIZE)):
                    try:
                        if src_rec[1][i] != etl_rec[1][i]:
                            diff_field.append(i)
                    except IndexError:
                        break
                print(len(diff_field))
                if len(diff_field) == 1:
                    
                    #compare_list.append((src_rec, etl_rec, diff_field))
                    if src_rec[0] not in compare_dict:
                        compare_dict[src_rec[0]] = []
                    compare_dict[src_rec[0]].append((src_rec, etl_rec,  diff_field)) 
        #print(compare_dict)
        if compare_dict:
            print('Diff in file:{}\n len_diff_etl:{}\n len_diff_src:{}\n'.format(filename,len(self.split_record_etl), len(self.split_record_src)))
            print(self.len_etl, self.len_src) 
        for comp in compare_dict.values():
           for etl_num in comp:
               print('num_rec_src:{} , num_rec_etl: {} \n'.format(etl_num[0][0], etl_num[1][0]))
               print('rec_src:{} \n rec_etl: {} \n\n'.format(etl_num[0][1], etl_num[1][1]))
               print('field diff {} \n\n'.format(etl_num[2]))

        

        unique_etl = self._get_unique_key(self.split_record_etl)
        unique_src = self._get_unique_key(self.split_record_src)
        dict_etl = self._create_dict_unique_dict(self.split_record_etl, unique_etl)
        dict_src = self._create_dict_unique_dict(self.split_record_src, unique_src)
        self.create_comparer_dict(dict_etl, dict_src)
        if len(self.set_etl - self.set_src) == 0:
            ReportAll.total_indentical+=self.len_etl
            ReportAll.total_len_etl+=self.len_etl
            ReportAll.total_len_etl+=self.len_src
            return
        self.etl_for_compare = etl
        self.src_for_compare = src
        if self.len_etl != self.len_src:
            if self.len_etl != len(self.set_etl):
                print('в эталонах повторяются строки')
                if self.len_src != len(self.set_src):
                    print('в сурсах повторяются строки')
                    self.src_for_compare = self.set_src
                else:
                    self.src_for_compare = src
        #self._set_num_records()
        self.split_record_etl = self._clear_split_record(self.etl_for_compare)
        self.split_record_src = self._clear_split_record(self.src_for_compare)
        unique_etl = self._get_unique_key(self.split_record_etl)
        unique_src = self._get_unique_key(self.split_record_src)
        dict_etl = self._create_dict_unique_dict(self.split_record_etl, unique_etl)
        dict_src = self._create_dict_unique_dict(self.split_record_src, unique_src)
        
        print()
        self.create_comparer_dict(dict_etl, dict_src)

    def create_comparer_dict(self, etl, src):
        #if etl.keys() == src.keys():
        for src_field, src_record in src.items():
        #for etl_field, etl_record in etl.items():
            #if src[etl_field] == etl_record:
            for etl_record in etl[src_field]:
                for value_etl in etl_record[1]:
                    pass

        else:
            pass




    

    def _set_num_records(self):
        self.src_for_compare = [(i, record) for i, record in enumerate(self.src_for_compare, 1)]
        self.etl_for_compare = [(i, record) for i, record in enumerate(self.etl_for_compare, 1)]

                    
    
    def _create_dict_unique_dict(self, records, unique):
        body_dict = {}
        for record in records:
            if record[1][unique] not in body_dict:
                body_dict[record[1][unique]] = []
            body_dict[record[1][unique]].append(record)
        return body_dict


            






    def difference_file(self, etl, src, filename):
        super()._prepare_etalon(etl, src)
        self.len_etl = len(self.etl)
        self.len_src = len(self.src)
        if self.full:
            header1, header2 = self.difference_parts(self.etl[:self.num_record_header], self.src[:self.num_record_header])
            body1, body2 = self.difference_parts(self.etl[self.num_record_header:-self.num_record_trailer], self.src[self.num_record_header:-self.num_record_trailer])
            trailer1, trailer2 = self.difference_parts(self.etl[-self.num_record_trailer:], self.src[-self.num_record_trailer:])
            return self.create_comparer((header1, header2), (body1, body2), (trailer1, trailer2))
        else:
            body1, body2 = self.difference_parts(self.etl, self.src)
            return self.create_comparer(body = (body1, body2))


    def difference_parts(self, etl, src):
        
        set1 = set([line for line in etl])
        set2 = set([line for line in src])
        len_diff_etl = self.len_etl - len(set1)
        len_diff_src = self.len_src - len(set2)
        if len_diff_etl != 0:
            print(f'Repeat records etl {len_diff_etl}')
        if len_diff_src != 0:
            print(f'Repeat records src {len_diff_src}')
        intersection = set1.intersection(set2) #пересечение
        difference_etl = set1.difference(set2) #элементы в etl
        difference_src = set2.difference(set1) #элементы в src
        if len(difference_etl) == 0 and len(difference_src) == 0:
            ReportAll.total_indentical+=len(etl)
            return
        else:
            return difference_etl, difference_src

    def create_comparer(self, header: tuple = (), body: tuple = (), trailer: tuple = (), filename : str = ""):
        #self.full = True
        header = None
        if header and body and trailer:
            header_etl, body_etl, trailer_etl = self._split_records_all(self.etl)
            header_src, body_src, trailer_src = self._split_records_all(self.src)
            header_diff_etl = self._clear_split_record(header[0])
            header_diff_src = self._clear_split_record(header[1])
            body_diff_etl = self._clear_split_record(body[0])
            body_diff_src = self._clear_split_record(body[1])
            self._get_unique_key(body_etl)

            trailer_diff_etl = self._clear_split_record(trailer[0])
            trailer_diff_src = self._clear_split_record(trailer[1])
            self.get_num_record_diff(header_etl, header_diff_etl)
            self.get_num_record_diff(body_etl, body_diff_etl)
            self.get_num_record_diff(body_src, body_diff_src)
            return CompareFactory(filename, 
                                  Header(header_etl, header_src, header_diff_etl, header_diff_src), 
                                  Body(body_etl, body_src, body_diff_etl, body_diff_src), 
                                  Trailer(trailer_etl, trailer_src, trailer_diff_etl, trailer_diff_src))
        elif body:
            body_etl = self._clear_split_record(self.etl)
            body_src = self._clear_split_record(self.src)
            body_diff_etl = self._clear_split_record(body[0])
            body_diff_src = self._clear_split_record(body[1])
            self._get_unique_key(body_etl)
            self.create_dict_keys(body_etl)
            self.body_diff_etl = self.get_num_record_diff(body_etl, body_diff_etl)
            self.body_diff_src = self.get_num_record_diff(body_src, body_diff_src)
            return CompareFactory(filename,
                                   body = Body(self.body_diff_etl, self.body_diff_src, self.unique_keys))
    


    def get_num_record_diff(self, all, diff):
        diff_res = []
        for rec_diff in diff:
            for rec_all in all:
                if rec_diff == rec_all[1]:
                    diff_res.append((rec_all))
        return diff_res

    def find_diff_etl(self, all, diff):
        for rec_diff in diff:
            i = 0
            for rec_num, rec_all in all:
                for key in self.unique_keys:
                    if rec_diff[1][key] == rec_all[key]:
                        i+=1

        


    def _split_records_all(self, records):
        header = self._clear_split_record(records[:self.num_record_header])
        body = self._clear_split_record(records[self.num_record_header:-self.num_record_trailer], self.num_record_header + 1)
        trailer = self._clear_split_record(records[-self.num_record_trailer:], len(body) + 2)
        return header, body, trailer
    
    def _get_unique_key(self, records_field):
        keys={}
        for i in range(len(setting.BODY_SIZE)):
            keys[i] = set()
            for record in records_field:
                try:
                    keys[i].add(record[1][i])
                except IndexError:
                    continue
                # if record[i] not in keys:
                #     #keys.add(record[i])
                #     
                # else: 
                #     keys = set()
                #     break 
            # if len(keys) == len(self.list_etl):
            #     unique_keys = True
            #     break
        self.unique_keys = tuple(dict(filter(lambda x: len(x[1]) > 2, keys.items())).keys())
        return max(keys.items(), key = lambda x: len(x[1]))[0]
        print()
        #self.unique_keys = filter(self.unique_keys, lambda x: )
        # if not unique_keys:
        #     FullFileComparer.num_key = 0
        #     return
        #FullFileComparer.num_key = unique_keys
        
    
    # def _split_records_all(self, records):
    #     header = self._clear_split_record(records[:self.num_record_header])
    #     body = self._clear_split_body(records[self.num_record_header:-self.num_record_trailer])
    #     trailer = self._clear_split_record(records[-self.num_record_trailer:])
    #     return header, body, trailer
    

    # def _clear_split_record(self, records, offset=1):
    #     compare_list = []
    #     for record in records:
    #         split_record = record.split(self.delimiter)
    #         records_list = []
    #         for field in split_record:
    #             records_list.append(field.strip())
    #         compare_list.append(records_list)
    #     return compare_list
    
    def _clear_split_record(self, records):
        compare_list = []
        #records.sort()
        for record in records:
            split_record = record[1].split(self.delimiter)
            #compare_dict[i] = []
            #compare_list.append((i,split_record))
            records_list = []
            for field in split_record:
                records_list.append(field.strip())
                #compare_dict[i].append(field.strip())
            compare_list.append((record[0],records_list))
        #compare_dict
        #compare_list.sort(key = lambda x : x[1])
        return compare_list


class FilePrepare(RecordPrepare):
    

    def __init__(self, size_header: list = [], 
                 size_body: list = [], 
                 size_trailer: list = [], 
                 num_records_header = 1, num_records_trailer = 2, 
                 full= False, prepare_etl = False) -> None:
        super().__init__(num_records_header, num_records_trailer, full, prepare_etl)
        self.size_header = size_header
        self.size_body = size_body
        self.size_trailer = size_trailer
        
    

    def create_comparer(self, etl, src, filename):
        super()._prepare_etalon(etl, src)
        if self.size_header and self.size_body and self.size_trailer:
            header1, body1, trailer1 = self._split_records_all(self.etl)
            header2, body2, trailer2 = self._split_records_all(self.src)
            return CompareFactory(filename, Header(header1, header2), Body(body1, body2), Trailer(trailer1, trailer2))
        elif self.size_body:
            body1 = self._clear_split_record(self.etl, self.size_body)
            body2 = self._clear_split_record(self.src, self.size_body)
            return CompareFactory(filename, body = Body(body1, body2))
        
    
    
    def _split_records_all(self, records):
        header = self._clear_split_record(records[:self.num_record_header], self.size_header)
        body = self._clear_split_record(records[self.num_record_header:-self.num_record_trailer], self.size_body, self.num_record_header + 1)
        trailer = self._clear_split_record(records[-self.num_record_trailer:], self.size_trailer, len(body) + 1)
        return header, body, trailer
    

    
    


    # def _clear_split_record(self, records, size_fields):
    #     compare_list = []
    #     for record in records:
    #         records_list = []
    #         start = 0
    #         for size_field in size_fields:
    #             records_list.append(record[start:start + size_field].strip())
    #             start+=size_field
    #         compare_list.append(records_list)
    #     return compare_list
    
    def _clear_split_record(self, records, size_fields, offset=1):
        compare_dict = {}
        for i, record in enumerate(records, offset):
            compare_dict[i] = []
            start = 0
            for size_field in size_fields:
                compare_dict[i].append(record[start:start + size_field].strip())
                start+=size_field
            #compare_list.append(records_list)
        return compare_dict



class FullFileComparer:
    num_key = 0

    def __init__(self, diff_etl, diff_src, unique_keys):
        #self.num_record = 1
        self.diff_etl = diff_etl
        self.diff_src = diff_src
        self.unique_keys = unique_keys
        #self.list_etl = etl
        #self.list_src = src
        #self.len_etl = len(etl)
        #self.len_src = len(src)
        self.total_matched = 0
        self.only_in_src = 0
        self.only_in_etl = 0
        self.broken_attributes = 0
        self.indentical = 0
        self.report = dict()
        self.in_etl_only  = []

    def compare_keys(self, names: list = []):
        self.etl_only = []
        for num_etl ,etls in self.list_etl.items():
            compare = False
            self.total_matched+=1
            for num_src, src in self.list_src.items():
                res = set(etls).difference(set(src)) 
                #res = len(set(etls) & set(src))
                if not res :
                    self.indentical+=1
                    compare = True
                    break
                elif len(res) == len(FullFileComparer.num_key):
                    pass
            if not compare:
                self.etl_only.append({num_etl :etls})


    def compare(self, names = []):
        compare_list= []
        for rec_src in self.diff_src:
            for rec_etl in self.diff_etl:
                for key in self.unique_keys:
                    i = 0
                    if rec_src[1][key] == rec_etl[1][key]:
                        i+=1
                if len(self.unique_keys) == i:
                    compare_list.append((rec_src[0], rec_etl[0]))
                break
        return compare_list
    #@abstractmethod
    # def compare(self, names: list = []):
    #     #self._get_records_only()
    #     set1 = set([(line_num, line) for line_num, line in self.list_etl])
    #     set2 = set([(line_num, line) for line_num, line in self.list_src])
    #     intersection = set1.intersection(set2)
    #     difference = set1.difference(set2)
    #     self.report['errors'] = {'num_field':  dict()}
    #     for field_etls, field_srcs, in zip(self.list_etl.items(), self.list_src.items()):
    #         self.total_matched+=1
    #         if field_etls[1] == field_srcs[1]:
    #             self.indentical+=1
    #         else:
    #             num_field = 0
    #             for field_etl, field_src in zip(field_etls[1], field_srcs[1]):
    #                 if field_etl != field_src:
    #                     if num_field in self.report['errors']['num_field']:
    #                         self.report['errors']['num_field'][num_field]['num_record'] = self.num_record 
    #                         self.report['errors']['num_field'][num_field]['num_record_etl'][field_etls[0]] = {
    #                                                                             'etl_value':field_etl,
    #                                                                             }
    #                         self.report['errors']['num_field'][num_field]['num_record_src'][field_srcs[0]] = {
    #                                                                             'src_value': field_src,
    #                                                                             }
    #                         self.report['errors']['num_field'][num_field]['name_field'] = names[num_field] if names[0] != "" else ""
    #                     else:
    #                         self.report['errors']['num_field'][num_field] = {'num_record_etl': {field_etls[0] : {
    #                                                                             'etl_value':field_etl}},
    #                                                                         'num_record_src':{field_srcs[0] : {
    #                                                                             'src_value': field_src}},
    #                                                                         'name_field':  names[num_field] if names[0] != "" else ""}
    #                     self.broken_attributes+=1
    #                 num_field+=1
    #         self.num_record+=1
    #     if not self.report['errors']['num_field']:
    #         self.report['errors'] = None
    #     self.report['statistics'] = dict()
    #     self.report['statistics']['indentical'] = self.indentical
    #     self.report['statistics']['broken_attributes'] = self.broken_attributes
    #     self.report['statistics']['total_matched'] = self.total_matched
    #     self.report['statistics']['only_in_etl'] = self.only_in_etl
    #     self.report['statistics']['only_in_src'] = self.only_in_src
    #     self.report['statistics']['len_etl'] = self.len_etl
    #     self.report['statistics']['len_src'] = self.len_src
    #     return self.report
                
    # def compare(self, names: list = []):
    #     self._get_records_only()
    #     self.report['errors'] = {'num_field':  dict()}
    #     offset = 1
    #     for i in range(start,len(self.list_etl)):
    #         self.total_matched+=1
    #         if self.list_etl[i] == self.list_src[offset]:
    #             self.indentical+=1
    #         else:
    #             self.compare(start = i, offset = offset)
    #             self.in_etl_only.append(self.list_etl[i])
    #         offset+=1



    

    
    def _get_records_only(self):
        if self.len_etl > self.len_src :
            self.only_in_etl = self.len_etl - self.len_src
        elif self.len_etl < self.len_src:
            self.only_in_src = self.len_src - self.len_etl


    def _get_unique_key(self, records_field):
        keys={}
        for i in range(len(setting.BODY_SIZE)):
            keys[i] = set()
            for record in records_field:
                try:
                    keys[i].add(record[1][i])
                except IndexError:
                    continue
                # if record[i] not in keys:
                #     #keys.add(record[i])
                #     
                # else: 
                #     keys = set()
                #     break 
            # if len(keys) == len(self.list_etl):
            #     unique_keys = True
            #     break
        self.unique_keys = tuple(dict(filter(lambda x: len(x[1]) > 2, keys.items())).keys())
        
    
    # def _get_unique_key(self):
    #     keys={}
    #     unique_keys = False
    #     for i in range(len(setting.BODY_SIZE)):
    #         keys[i] = set()
    #         for record in self.list_etl.values():
    #             try:
    #                 keys[i].add(record[i])
    #             except IndexError:
    #                 continue
    #             # if record[i] not in keys:
    #             #     #keys.add(record[i])
    #             #     
    #             # else: 
    #             #     keys = set()
    #             #     break 
    #         # if len(keys) == len(self.list_etl):
    #         #     unique_keys = True
    #         #     break
    #     asd = tuple(dict(sorted(keys.items(), key= lambda x: len(x[1]))[-4:]).keys())
    #     # if not unique_keys:
    #     #     FullFileComparer.num_key = 0
    #     #     return
    #     FullFileComparer.num_key = asd
        
 

    
        

# class Header(FullFileComparer):


#     def __init__(self, diff_etl, diff_src, unique_keys) -> None:
#         super().__init__(diff_etl, diff_src, unique_keys)


# class Body(FullFileComparer):


#     def __init__(self, diff_etl, diff_src, unique_keys) -> None:
#         #etl = dict(filter(lambda x:x[1][0] != '111', etl.items()))
#         super().__init__(diff_etl, diff_src, unique_keys)


# class Trailer(FullFileComparer):


#     def __init__(self, diff_etl, diff_src, unique_keys) -> None:
#         super().__init__(diff_etl, diff_src, unique_keys)
    




class CompareFactory:

    def __init__(self, filename: str, header: Header = None, body: Body= None, trailer: Trailer = None, ) -> None:
        self.header = header
        self.body = body
        self.trailer = trailer
        self.report = dict()
        self.filename = filename


    def compare(self):
        if self.header:
            self.report['header'] = self.header.compare(setting.HEADER_NAMES)
        if self.body:
            self.report['body'] = self.body.compare(setting.BODY_NAMES)
        if self.trailer:
            self.report['trailer'] = self.trailer.compare(setting.TRAILER_NAMES)
        return {self.filename:self.report}
                

class FileReader:

    def __init__(self, path, filename):
        with codecs.open(join(path, filename), 'r', encoding='UTF-8') as f:
            self.records = f.readlines()
        



class Report:
    list_compares= []
    date = datetime.now().date()
    total_len_etl = 0
    total_len_src = 0
    total_matched = 0
    total_only_in_etl = 0
    total_only_in_src = 0
    total_broken_attributes = 0
    total_indentical = 0
    total_errors = {}

        

    @classmethod
    def to_excel(cls):
        pass
                                 




def main(etalon_path, source_path, list_files):
    # if exists(FILENAME_FORMAT):
    #     with open(FILENAME_FORMAT, "r") as file:
    #         formats = json.load(file)
    #         header_format =  formats['header'] if isinstance(formats['header'][0], int) else None
    #         try:
    #             body_format =  formats['body']['name_field']
    #             return print("Заполнить файл formats.json")
    #         except KeyError:
    #             body_format =  formats['body']
    #         trailer_format =  formats['header'] if isinstance(formats['trailer'][0], int) else None
    #         full_compare = True if (header_format and trailer_format) else False
    # else:
    #     with open(FILENAME_FORMAT, "w") as file:
    #         json.dump(FILE_STRUCTURE, file)
    #     return print("Заполнить файл formats.json")
    # if setting.TYPE_DELIMITER == 'char':
    #     filePrerare = FilePrepareDel(setting.DELIMITER, prepare_etl=False)

    # elif setting.TYPE_DELIMITER == 'fields':
    #     filePrerare = FilePrepare(setting.HEADER_SIZE, setting.BODY_SIZE, setting.TRAILER_SIZE,  prepare_etl=False)
    # else:
    #     return
    for filename in list_files:
        counter = Counter()
        etlReader = FileReader(etalon_path, filename)
        srcReader = FileReader(source_path, filename)
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
    return len(list_files)
       

def compare_multithreading(path_etl, path_src, list_files):
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
            results = exe.submit(main, path_etl, path_src, filenames)
            print(results.result())

    
files = FilesCollection(setting.ETL, setting.SRC)
list_files = files.get_list_files()

if len(list_files) > 100:
    compare_multithreading(files.path_etl, files.path_src, list_files)
else:
    main(files.path_etl, files.path_src, list_files)
ReportAll.write_total_record()

        

