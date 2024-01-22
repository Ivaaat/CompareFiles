import json
import setting
import csv
from datetime import datetime
import codecs
from os.path import join, isfile, isdir, exists, basename
from concurrent.futures import ThreadPoolExecutor
import os
from abc import ABC, abstractmethod


class FileFolderPrepare:


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

class Prepare:

    def __init__(self, num_records_header, num_records_trailer , 
                 full, prepare_etl) -> None:
        self.full = full
        self.prepare_etl = prepare_etl
        self.num_record_header = num_records_header
        self.num_record_trailer = num_records_trailer

    def _prepare_etalon(self, etl, src):
        self.etl = etl
        self.src = src
        if self.prepare_etl:
            self.etl = etl[self.num_record_header:-self.num_record_trailer]


        

class FilePrepareDel(Prepare):
    def __init__(self, delimiter, num_records_header = 1, num_records_trailer = 2, 
                 full= False, prepare_etl = False) -> None:
        super().__init__(num_records_header, num_records_trailer, full, prepare_etl)
        self.delimiter = delimiter


    def create_comparer(self, etl, src, filename):
        super()._prepare_etalon(etl, src)
        if self.full:
            header1, body1, trailer1 = self._split_records_all(self.etl)
            header2, body2, trailer2 = self._split_records_all(self.src)
            return CompareFactory(filename, Header(header1, header2), Body(body1, body2, self.num_record_header), Trailer(trailer1, trailer2, len(body1)))
        else:
            body1 = self._clear_split_record(self.etl)
            body2 = self._clear_split_record(self.src)
            return CompareFactory(filename, body = Body(body1, body2))
        
    
    def _split_records_all(self, records):
        header = self._clear_split_record(records[:self.num_record_header])
        body = self._clear_split_record(records[self.num_record_header:-self.num_record_trailer])
        trailer = self._clear_split_record(records[-self.num_record_trailer:])
        return header, body, trailer
    

    def _clear_split_record(self, records):
        compare_list = []
        for record in records:
            split_record = record.split(self.delimiter)
            records_list = []
            for field in split_record:
                records_list.append(field.strip())
            compare_list.append(records_list)
        return compare_list


class FilePrepare(Prepare):
    

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
            return CompareFactory(filename, Header(header1, header2), Body(body1, body2, self.num_record_header), Trailer(trailer1, trailer2, len(body1)))
        elif self.size_body:
            body1 = self._clear_split_record(self.etl, self.size_body)
            body2 = self._clear_split_record(self.src, self.size_body)
            return CompareFactory(filename, body = Body(body1, body2))
    

    def _split_records_all(self, records):
        header = self._clear_split_record(records[:self.num_record_header], self.size_header)
        body = self._clear_split_record(records[self.num_record_header:-self.num_record_trailer], self.size_body)
        trailer = self._clear_split_record(records[-self.num_record_trailer:], self.size_trailer)
        return header, body, trailer
    


    def _clear_split_record(self, records, size_fields):
        compare_list = []
        for record in records:
            records_list = []
            start = 0
            for size_field in size_fields:
                records_list.append(record[start:start + size_field].strip())
                start+=size_field
            compare_list.append(records_list)
        return compare_list



class FullFileComparer:
    num_key = 0

    def __init__(self, etl: list, src: list):
        self.num_record = 1
        self.list_etl = etl
        self.list_src = src
        self.len_etl = len(etl)
        self.len_src = len(src)
        self.total_matched = 0
        self.only_in_src = 0
        self.only_in_etl = 0
        self.broken_attributes = 0
        self.indentical = 0
        self.report = dict()



    #@abstractmethod
    def compare(self, names: list = []):
        self._get_records_only()
        self.report['errors'] = {'num_field':  dict()}
        for field_etls, field_srcs in zip(self.list_etl, self.list_src):
            self.total_matched+=1
            if field_etls == field_srcs:
                self.indentical+=1
            else:
                num_field = 0
                for field_etl, field_src in zip(field_etls, field_srcs):
                    if field_etl != field_src:
                        if num_field in self.report['errors']['num_field']:
                            #self.report['errors']['num_field'][num_field]['num_record'] = self.num_record 
                            self.report['errors']['num_field'][num_field]['num_record'][self.num_record] = {
                                                                                'etl_value':field_etl,
                                                                                'src_value': field_src
                                                                                }
                        else:
                            self.report['errors']['num_field'][num_field] = {'num_record':{self.num_record : {
                                                                                'etl_value':field_etl,
                                                                                'src_value': field_src},
                                                                            },
                                                                            'name_field':  names[num_field] if names[0] != "" else ""}
                        self.broken_attributes+=1
                    num_field+=1
            self.num_record+=1
        if not self.report['errors']['num_field']:
            self.report['errors'] = None
        self.report['statistics'] = dict()
        self.report['statistics']['indentical'] = self.indentical
        self.report['statistics']['broken_attributes'] = self.broken_attributes
        self.report['statistics']['total_matched'] = self.total_matched
        self.report['statistics']['only_in_etl'] = self.only_in_etl
        self.report['statistics']['only_in_src'] = self.only_in_src
        self.report['statistics']['len_etl'] = self.len_etl
        self.report['statistics']['len_src'] = self.len_src
        return self.report


    

    
    def _get_records_only(self):
        if self.len_etl > self.len_src :
            self.only_in_etl = self.len_etl - self.len_src
        elif self.len_etl < self.len_src:
            self.only_in_src = self.len_src - self.len_etl

    
    def _get_unique_key(self):
        keys=set()
        unique_keys = False
        for i in range(len(self.list_etl[0])):
            for record in self.list_etl:
                if record[i] not in keys:
                    keys.add(record[i])
                else: 
                    keys = set()
                    break 
            if len(keys) == len(self.list_etl):
                unique_keys = True
                break
        if not unique_keys:
            FullFileComparer.num_key = -1
            return
        FullFileComparer.num_key = i
        
 

    
        

class Header(FullFileComparer):

    def __init__(self, etl: list, src: list) -> None:
        super().__init__(etl, src)



class Body(FullFileComparer):

    def __init__(self, etl: list, src: list, num_record_header = 0) -> None:
        super().__init__(etl, src)
        self.num_record = num_record_header + 1


class Trailer(FullFileComparer):


    def __init__(self, etl: list, src: list, num_record_body = 0) -> None:
        super().__init__(etl, src)
        self.num_record = num_record_body
    




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
            if FullFileComparer.num_key == 0:
                self.body._get_unique_key()
            if FullFileComparer.num_key > 0:
                self.list_etl = self.body.list_etl.sort(key = lambda row: row[FullFileComparer.num_key])
                self.list_src = self.body.list_src.sort(key = lambda row: row[FullFileComparer.num_key])
            self.report['body'] = self.body.compare(setting.BODY_NAMES)
        if self.trailer:
            self.report['trailer'] = self.trailer.compare(setting.TRAILER_NAMES)
        return {self.filename:self.report}
                

class GetRecordsFile:

    def __init__(self, path, file):
        with codecs.open(join(path, file), 'r+', encoding='UTF-8') as f:
            self.records = f.readlines()

class ReportAll:
    date = datetime.now().date()
    total_len_etl = 0
    total_len_src = 0
    total_matched = 0
    total_only_in_etl = 0
    total_only_in_src = 0
    total_broken_attributes = 0
    total_indentical = 0
    total_errors = {}
    comparison_header = 'Comparing file;Date;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Broken attributes same ID;Identical\n'
    if not exists(setting.RES):
        os.mkdir(setting.RES)
    with open(join(setting.RES, f'result_{str(datetime.now().date())}.csv'), 'w+') as csv_file: 
        csv_file.write(comparison_header)


    def __init__(self, comparer) -> None:
        self.filename = comparer.filename
        self.errors = {part_name:error['errors'] for part_name, error in comparer.report.items() if error['errors'] != None}
        self.totals = [total['statistics'] for total in comparer.report.values()]
        self.record_len_etl = 0
        self.record_len_src = 0
        self.record_matched = 0
        self.record_only_in_etl = 0
        self.record_only_in_src = 0
        self.record_broken_attributes = 0
        self.record_indentical = 0
    
    
    def create_file_report_csv(self):
        with open(join(setting.RES, f'result_{self.date}.csv'), 'a+', newline='') as csv_file: 
            csv_writer = csv.writer(csv_file, delimiter = ';')
            for part_value in self.totals:
                self.record_len_etl +=part_value['len_etl']
                self.record_len_src +=part_value['len_src']
                self.record_matched +=part_value['total_matched']
                self.record_only_in_etl +=part_value['only_in_etl']
                self.record_only_in_src +=part_value['only_in_src']
                self.record_broken_attributes +=part_value['broken_attributes']
                self.record_indentical +=part_value['indentical']
            ReportAll.total_len_etl+=self.record_len_etl
            ReportAll.total_len_src+=self.record_len_src
            ReportAll.total_matched+=self.record_matched
            ReportAll.total_only_in_etl+=self.record_only_in_etl
            ReportAll.total_only_in_src+=self.record_only_in_src
            ReportAll.total_broken_attributes+=self.record_broken_attributes
            ReportAll.total_indentical+=self.record_indentical
            csv_writer.writerow([self.filename, 
                                    datetime.now(),
                                    self.record_len_etl,
                                    self.record_len_src,
                                    self.record_matched,
                                    self.record_only_in_etl,
                                    self.record_only_in_src,
                                    self.record_broken_attributes,
                                    self.record_indentical])   
                

    def create_file_errors_report(self):
        for part_name, errors in self.errors.items():
            res_folder = join(f"{setting.RES}_{str(self.date)}")
            if not exists(res_folder):
                os.mkdir(res_folder)
            part_folder = join(res_folder, part_name)
            if not exists(part_folder):
                os.mkdir(part_folder)
            for num_fields, num_records  in errors['num_field'].items():
                with open(join(part_folder, f'num_fields_{num_fields}_{num_records['name_field']}.report'), 'w+') as report_file: 
                    report_file.write("{};\n".format(self.filename))
                    for num_record,value  in num_records['num_record'].items():
                        report_file.write("  num_record: {};\n".format(num_record))
                        for name, value_field in value.items():
                            report_file.write("   {} : {}\n".format(name, value_field))
    

    @classmethod
    def write_total_record(cls):
        with open(join(setting.RES, f'result_{cls.date}.csv'), 'a+') as csv_file: 
            csv_writer = csv.writer(csv_file, delimiter= ';')
            csv_writer.writerow(['TOTAL', 
                                datetime.now(),
                                cls.total_len_etl,
                                cls.total_len_src,
                                cls.total_matched,
                                cls.total_only_in_etl,
                                cls.total_only_in_src,
                                cls.total_broken_attributes,
                                cls.total_indentical,
                                '{0:.2f}%'.format((cls.total_indentical/cls.total_len_etl)*100),
                                '{0:.2f}%'.format((cls.total_broken_attributes/cls.total_len_etl)*100)])
    

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
    if setting.TYPE_DELIMITER == 'char':
        filePrerare = FilePrepareDel(setting.DELIMITER, prepare_etl=False)

    elif setting.TYPE_DELIMITER == 'fields':
        filePrerare = FilePrepare(setting.HEADER_SIZE, setting.BODY_SIZE, setting.TRAILER_SIZE,  prepare_etl=False)
    else:
        return
    for filename in list_files:
        etl = GetRecordsFile(etalon_path, filename)
        src = GetRecordsFile(source_path, filename)
        comparer = filePrerare.create_comparer(etl.records, src.records, filename)
        comparer.compare()
        reportAll = ReportAll(comparer)
        if reportAll.errors:
            reportAll.create_file_errors_report()
        reportAll.create_file_report_csv()
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

    


files = FileFolderPrepare(setting.ETL, setting.SRC)
list_files = files.get_list_files()

if len(list_files) > 100:
    compare_multithreading(files.path_etl, files.path_src, list_files)
else:
    main(files.path_etl, files.path_src, list_files)
ReportAll.write_total_record()

        

