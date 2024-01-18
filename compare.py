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
    file_list_compare = []


    def __init__(self, etl: str, src :str) -> None:
        self.etl = etl
        self.src = src
        

    def get_list_files(self) -> list: 
        if isdir(self.etl) and isdir(self.src):
            self.path_etl = os.path.abspath(self.etl)
            self.path_src = os.path.abspath(self.src)
            for file_etl, file_src in zip(os.listdir(self.path_etl), os.listdir(self.path_src)):
                self.prepare_name(file_etl, file_src)
        elif isfile(self.etl) and isfile(self.src):
            self.path_etl, self.etl = os.path.split(os.path.abspath(self.etl))
            self.path_src, self.src = os.path.split(os.path.abspath(self.src))
            self.prepare_name(self.etl, self.src)
        else:
            print("Сравнить можно либо папки, либо файлы")
        return self.file_list_compare
    

    def prepare_name(self, etl_name, src_name):
        if etl_name == src_name:
            self.file_list_compare.append(etl_name)
        else:
            print("Нужно выражения для обработки названия файлов")


class FilePrepare:
    

    def __init__(self, size_header: list = [], 
                 size_body: list = [], 
                 size_trailer: list = [], 
                 num_records_header = 1, num_records_trailer = 2, 
                 full = False, prepare_etl= False) -> None:
        self.size_header = size_header
        self.size_body = size_body
        self.size_trailer = size_trailer
        self.num_record_header = num_records_header
        self.num_record_trailer = num_records_trailer
        self.prepare_etl = prepare_etl
        self.full = full
    

    def create_comparer(self, etl, src, filename):
        if self.prepare_etl:
            etl = etl[self.num_record_header:-self.num_record_trailer]
        if self.full:
            header1, body1, trailer1 = self._split_records_all(etl)
            header2, body2, trailer2 = self._split_records_all(src)
            return CompareFactory(filename, Header(header1, header2), Body(body1, body2, self.num_record_header), Trailer(trailer1, trailer2, len(body1)))
        else:
            body1 = self._split_records_body(etl)
            body2 = self._split_records_body(src)
            return CompareFactory(filename, body = Body(body1, body2))
    
    def _split_records_all(self, records):
        header = self._clear_split_record(records[:self.num_record_header], self.size_header)
        body = self._clear_split_record(records[self.num_record_header:-self.num_record_trailer], self.size_body)
        trailer = self._clear_split_record(records[-self.num_record_trailer:], self.size_trailer)
        return header, body, trailer


    def _split_records_body(self, records):
        body = self._clear_split_record(records, self.size_body)
        return body


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



class FullFileComparer(ABC):

    def __init__(self, etl: list, src: list):
        self.num_record = 1
        self.list_etl = etl
        self.list_src = src
        self.len_etl = len(etl)
        self.len_src = len(src)
        self.report = dict()
        self.total_matched = 0
        self.only_in_src = 0
        self.only_in_etl = 0
        self.broken_attributes = 0
        self.indentical = 0
        self.part_name = ''


    @abstractmethod
    def compare(self):
        self._get_records_only()
        self.report['errors'] = {'num_record':  dict()}
        for field_etls, field_srcs in zip(self.list_etl, self.list_src):
            self.total_matched +=1
            if field_etls == field_srcs:
                self.indentical+=1
            else:
                self.report['errors']['num_record'][self.num_record] =  {'num_field': dict()}
                num_field= 0
                for field_etl, field_src in zip(field_etls, field_srcs):
                    if field_etl != field_src:
                        self.broken_attributes+=1
                        self.report['errors']['num_record'][self.num_record]['num_field'][num_field] = {
                                                                                'etl_value':field_etl,
                                                                                'src_value': field_src,
                                                                                }
                num_field+=1
            self.num_record+=1
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

    
        

class Header(FullFileComparer):

    def __init__(self, etl: list, src: list) -> None:
        super().__init__(etl, src)

        

    def compare(self):
        return super().compare()


class Body(FullFileComparer):

    def __init__(self, etl: list, src: list, num_record_header = 0) -> None:
        super().__init__(etl, src)
        self.num_record = num_record_header + 1


    def compare(self, body: dict):
        super()._get_records_only()
        self.report['errors'] = {'num_record':  dict()}
        for field_etls, field_srcs in zip(self.list_etl, self.list_src):
            self.total_matched+=1
            if field_etls == field_srcs:
                self.indentical+=1
            else:
                self.report['errors']['num_record'][self.num_record] =  {'num_field': dict()}
                for i, name_field in enumerate(body):
                    try:
                        if field_etls[i] != field_srcs[i]:
                            self.report['errors']['num_record'][self.num_record]['num_field'][i] = {
                                                                                'name_field' : name_field,
                                                                                'etl_value':field_etls[i],
                                                                                'src_value': field_srcs[i]
                                                                                }
                            self.broken_attributes+=1
                    except IndexError:
                        break
            self.num_record+=1
        self.report['statistics'] = dict()
        self.report['statistics']['indentical'] = self.indentical
        self.report['statistics']['broken_attributes'] = self.broken_attributes
        self.report['statistics']['total_matched'] = self.total_matched
        self.report['statistics']['only_in_etl'] = self.only_in_etl
        self.report['statistics']['only_in_src'] = self.only_in_src
        self.report['statistics']['len_etl'] = self.len_etl
        self.report['statistics']['len_src'] = self.len_src
        return self.report


class Trailer(FullFileComparer):


    def __init__(self, etl: list, src: list, num_record_body = 0) -> None:
        super().__init__(etl, src)
        self.num_record = num_record_body
    

    def compare(self):
        return super().compare()



class CompareFactory:

    def __init__(self, filename: str, header: Header = None, body: Body= None, trailer: Trailer = None, ) -> None:
        self.header = header
        self.body = body
        self.trailer = trailer
        self.report = dict()
        self.filename = filename


    def compare(self, body_name_fields):
        if self.header:
            self.report['header'] = self.header.compare()
        if self.body:
            self.report['body'] = self.body.compare(body_name_fields)
        if self.trailer:
            self.report['trailer'] = self.trailer.compare()
        return {self.filename:self.report}
                

class GetRecordsFile:

    def __init__(self, path, file):
        with codecs.open(join(path, file), 'r+', encoding='UTF-8') as f:
            self.records = f.readlines()

class ReportAll:
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
    comparison_header = 'Comparing file;Date;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Broken attributes same ID;Identical\n'


    def __init__(self, comparer, path_result) -> None:
        self.path_result = path_result
        self.filename = comparer.filename
        self.errors = [error['errors'] for error in comparer.report.values()]
        self.totals = [total['statistics'] for total in comparer.report.values()]
    
    def create_file_report_csv(self):
        if not exists(self.path_result):
            os.mkdir(self.path_result)
        # with open(join(self.path_result, f'result_{self.date}.csv'), 'a+') as csv_file: 
        #     csv_file.write(self.comparison_header)
        with open(join(self.path_result, f'result_{self.date}.csv'), 'a+') as csv_file: 
            csv_file.write(self.comparison_header)
            csv_writer = csv.writer(csv_file)
            total_len_etl = 0
            total_len_src = 0
            total_matched = 0
            total_only_in_etl = 0
            total_only_in_src = 0
            total_broken_attributes = 0
            total_indentical = 0
            for part_value in self.totals:
                total_len_etl +=part_value['len_etl']
                total_len_src +=part_value['len_src']
                total_matched +=part_value['total_matched']
                total_only_in_etl +=part_value['only_in_etl']
                total_only_in_src +=part_value['only_in_src']
                total_broken_attributes +=part_value['broken_attributes']
                total_indentical +=part_value['indentical']
            self.total_len_etl+=total_len_etl
            self.total_len_src+=total_len_etl
            self.total_matched+=total_matched
            self.total_only_in_etl+=total_only_in_etl
            self.total_only_in_src+=total_only_in_src
            self.total_broken_attributes+=total_broken_attributes
            self.total_indentical+=total_indentical
            csv_writer.writerow([self.filename, 
                                    total_len_etl,
                                    total_len_src,
                                    total_matched,
                                    total_only_in_etl,
                                    total_only_in_src,
                                    total_broken_attributes,
                                    total_indentical])   
            
                


    def create_file_report(self):
        with open( f'filename_{self.filename}', 'a+') as report_file: 
            for errors in self.errors:
                for num_record, num_fields in errors['num_record'].items():
                    report_file.write("\nnum_record: {};\n".format(num_record))
                    for num_field, value in num_fields['num_field'].items():
                        report_file.write("  num_field: {};\n".format(num_field))
                        for name, value_field in value.items():
                            report_file.write("   {} : {}\n".format(name, value_field))
    

    def write_total_record(self):
        with open(join(self.path_result, f'result_{self.date}.csv'), 'a+') as csv_file: 
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['TOTAL', 
                                self.total_len_etl,
                                self.total_len_src,
                                self.total_matched,
                                self.total_only_in_etl,
                                self.total_only_in_src,
                                self.total_broken_attributes,
                                self.total_indentical])  
    

class TotalReport(ReportAll):

    def __init__(self, totals) -> None:
        self.totals = totals
        self.len_etl = 0
        self.len_src = 0
        self.matched = 0
        self.only_in_etl = 0
        self.only_in_src = 0
        self.broken_attributes = 0
        self.indentical = 0
    
    def create_file_report_csv(self):
        if not exists(self.path_result):
            os.mkdir(self.path_result)
        # with open(join(self.path_result, f'result_{self.date}.csv'), 'a+') as csv_file: 
        #     csv_file.write(self.comparison_header)
        with open(join(self.path_result, f'result_{self.date}.csv'), 'a+') as csv_file: 
            csv_file.write(self.comparison_header)
            csv_writer = csv.writer(csv_file)
            total_len_etl = 0
            total_len_src = 0
            total_matched = 0
            total_only_in_etl = 0
            total_only_in_src = 0
            total_broken_attributes = 0
            total_indentical = 0
            for part_value in self.totals:
                total_len_etl +=part_value['len_etl']
                total_len_src +=part_value['len_src']
                total_matched +=part_value['total_matched']
                total_only_in_etl +=part_value['only_in_etl']
                total_only_in_src +=part_value['only_in_src']
                total_broken_attributes +=part_value['broken_attributes']
                total_indentical +=part_value['indentical']
            self.total_len_etl+=total_len_etl
            self.total_len_src+=total_len_etl
            self.total_matched+=total_matched
            self.total_only_in_etl+=total_only_in_etl
            self.total_only_in_src+=total_only_in_src
            self.total_broken_attributes+=total_broken_attributes
            self.total_indentical+=total_indentical
            csv_writer.writerow([self.filename, 
                                    total_len_etl,
                                    total_len_src,
                                    total_matched,
                                    total_only_in_etl,
                                    total_only_in_src,
                                    total_broken_attributes,
                                    total_indentical])   



class ErrorReport(ReportAll):

    def __init__(self) -> None:
        pass

    
    def create_file_report(self):
        with open( f'filename_{self.filename}', 'a+') as report_file: 
            for errors in self.errors:
                for num_record, num_fields in errors['errors']['num_record'].items():
                    report_file.write("\nnum_record: {};\n".format(num_record))
                    for num_field, value in num_fields['num_field'].items():
                        report_file.write("  num_field: {};\n".format(num_field))
                        for name, value_field in value.items():
                            report_file.write("   {} : {}\n".format(name, value_field))
                    






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
                                 


def thread(etalon_path, source_path, list_files):
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
    full_compare = True
    
    #filePrerare = FilePrepare(header_format, body_format, trailer_format, full=full_compare, prepare_etl=False)
    # n_workers = 10
    # chunksize = round(len(list_files) / n_workers)
    # with concurrent.futures.ThreadPoolExecutor(n_workers) as exe:
    #     for i in range(0, len(list_files), chunksize):
    #         filenames = list_files[i:(i + chunksize)]
    #         results = exe.submit(move_files, filenames)
    #         print(results.result())
    filePrerare = FilePrepare(setting.HEADER_SIZE, setting.BODY_SIZE, setting.TRAILER_SIZE, full=full_compare, prepare_etl=False)
    for filename in list_files:
        etl = GetRecordsFile(etalon_path, filename)
        src = GetRecordsFile(source_path, filename)
        comparer = filePrerare.create_comparer(etl.records, src.records, filename)
        comparer.compare(setting.BODY_NAMES)
        reportAll = ReportAll(comparer, setting.RES)
        reportAll.create_file_report()
        reportAll.create_file_report_csv()
    reportAll.write_total_record()
        # reportErrorFile = ErrorReport()
        # reportErrorFile.create_file_report()
        # totalReportFile = TotalReport()
        # totalReportFile.create_file_report('res')
        #Report.list_compares.append(comparer)

    #Report.to_excel('res')
def main(path_etl, path_src, list_files):
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     futures = []
    #     for i in range(10):
    #         futures.append(executor.submit(comparator.compare_files, f"file1_{i}.txt", f"file2_{i}.txt"))
    #     for future in concurrent.futures.as_completed(futures):
    #         future.result()
    n_workers = 10
    #chunksize = round(len(list_files) / n_workers)
    chunksize = 1
    # create the process pool
    with ThreadPoolExecutor(n_workers) as exe:
        #results = list(exe.map(move_files, files))
        # split the move operations into chunks
        for i in range(0, len(list_files), chunksize):
            #print(i)
            # select a chunk of filenames
            filenames = list_files[i:(i + chunksize)]
            results = exe.submit(thread, path_etl, path_src, filenames)
            print(results.result())


files = FileFolderPrepare(setting.ETL, setting.SRC)
list_files = files.get_list_files()

thread(files.path_etl, files.path_src, list_files)

        

