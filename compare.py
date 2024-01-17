import csv
from datetime import datetime
import codecs
from os.path import join, isfile, isdir, exists, basename
import os
from abc import ABC, abstractmethod

FILE1 = 'etl.txt'
FILE2 = 'src.txt'

size_fields = [6,5,5,14,10,10,21,21,21,31,31,17,10,10,5,10,5,10,3,10,10,1,1,1,1,10,20,20,10,10,5,5,5,15,15]

# with codecs.open(FILE1, 'r+', encoding='UTF-8') as etl:
#     #etl_list = etl.splitlines()
#     etl_list = etl.readlines()


def split_file_lists_fields(file, size_fields):
    compare_list = []
    with codecs.open(file, 'r+', encoding='UTF-8') as f:
        lines_records = f.readlines()
    for record in lines_records:
        records_list = []
        start = 0
        for size_field in size_fields:
            records_list.append(record[start:start + size_field].strip())
            start+=size_field
        compare_list.append(records_list)
        #print(fields_etl)
    return compare_list

#print(len(split_file_lists_fields(FILE1, size_fields)))


class FileFolderPrepare:
    file_list_compare = []


    def __init__(self, etl: str, src :str) -> None:
        self.etl = etl
        self.src = src
        
        # self.path_etl = os.path.abspath(etl)
        # self.path_src = os.path.abspath(src)
    

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


        




class FileComparator:
    

    def __init__(self, size_header: list = [], size_body: dict= dict(), size_trailer: list = [], num_records_header = 1, num_records_trailer = 2) -> None:
        self.size_header = size_header
        self.size_body = list(size_body.values())
        self.size_trailer = size_trailer
        self.num_record_header = num_records_header
        self.num_record_trailer = num_records_trailer
    

    def create_comparer(self, etl, src, filename, full):
        if full:
            header1, body1, trailer1 = self._split_records_all(etl)
            header2, body2, trailer2 = self._split_records_all(src)
            # header_etl = Header()
            # header_etl.clear_split_records(etl[:2], self.size_header)
            # body_etl = Body()
            # body_etl.clear_split_records(etl, self.size_body)
            # trailer_etl = Trailer()
            # trailer_etl.clear_split_records(etl[:-2], self.size_trailer)


            # header_src = Header()
            # header_src.clear_split_records(self.size_header[:2])
            # body_src = Body()
            # body_src.clear_split_records(self.size_body)
            # trailer_src = Trailer()
            # trailer_src.clear_split_records(self.size_trailer[:-2])

            # self.etl = FullFilePrepare(etl, self.size_header, self.size_body, self.size_trailer)
            # self.src = FullFilePrepare(src, self.size_header, self.size_body, self.size_trailer)
        else:
            # self.etl = WithoutHeaderTrailerPrepare(etl, self.size_body)
            # self.src = WithoutHeaderTrailerPrepare(src, self.size_body)
            body1 = self._split_records_all(etl)
            body2 = self._split_records_all(src)
            return FullFileComparer(self.etl.compare_list, self.src.compare_list, filename)
    
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


class FullFilePrepare(ABC):

    def __init__(self) -> None:
        self.compare_list = []


    @abstractmethod
    def clear_split_records(self, records, size_fields):
        for record in records:
            records_list = []
            start = 0
            for size_field in size_fields:
                records_list.append(record[start:start + size_field].strip())
                start+=size_field
        self.compare_list.append(records_list)






# class FullFilePrepare:

#     def __init__(self, records, size_header, size_body, size_trailer):
#         self.compare_list = []
#         for i, record in enumerate(records):
#             records_list = []
#             start = 0
#             if i < 1:
#                 for size_field in size_header:
#                     records_list.append(record[start:start + size_field].strip())
#                     start+=size_field
#             elif i >= 1 and i < (len(records) - 2):
#                 for size_field in size_body:
#                     records_list.append(record[start:start + size_field].strip())
#                     start+=size_field
#                 self.compare_list.append(records_list)
#             else:
#                 for size_field in size_trailer:
#                     records_list.append(record[start:start + size_field].strip())
#                     start+=size_field
#             self.compare_list.append(records_list)
#             i+=1


class FullFileComparer(ABC):
    
    @abstractmethod
    def __init__(self, etl: list, src: list, filename: str):
        self.list_etl = etl
        self.list_src = src
        self.filename = filename
        self.report = dict()
        self.date = datetime.now()
        self.total_matched = 0
        self.broken_attributes = 0
        self.indentical = 0




    def compare(self, body: dict):
        self.report[self.filename] = []
        for field_etls, field_srcs in zip(self.list_etl, self.list_src):
            self.total_matched +=1
            if field_etls == field_srcs:
                self.indentical+=1
            else:
                for i, name_field in enumerate(body):
                    try:
                        if field_etls[i] != field_srcs[i]:
                            self.broken_attributes+=1
                            self.report[self.filename].append({'errors': {'name_field': name_field,
                                                                        'num_field' : i}})
                    except IndexError:
                        break
        self.num_etl = len(self.list_etl)
        self.num_src = len(self.list_src)
        

class Header(FullFileComparer):

    def __init__(self) -> None:
        super().__init__()


    def clear_split_records(self, records, size_header):
        super.clear_split_records(records, size_header)


class Trailer(FullFilePrepare):


    def clear_split_records(self, records, size_trailer):
        super.clear_split_records(records, size_trailer)


class Body(FullFilePrepare):


    def clear_split_records(self, records, size_body):
        super.clear_split_records(records, list(size_body.values()))
        
class GetRecordsFile:

    def __init__(self, path, file):
        self.records = []
        with codecs.open(join(path, file), 'r+', encoding='UTF-8') as f:
            self.records = f.readlines()


class WithoutHeaderTrailerPrepare:

    def __init__(self, records):
        self.compare_list = []
        for record in records:
            records_list = []
            start = 0
            for size_field in size_fields:
                records_list.append(record[start:start + size_field].strip())
                start+=size_field
            self.compare_list.append(records_list)


class Report:
    list_compares: FullFileComparer  = []

    @classmethod
    def to_excel(cls, path_result):
        if not exists(path_result):
            os.mkdir(path_result)
        comparison_header = 'Comparing file;Date;Records in etalon;Records in src;Total matched by line ID;In etalon only;In src only;Broken attributes same ID;Identical'
        date_matching = cls.list_compares[-1].date
        with open(join(path_result, f'result_{str(date_matching.date())}.csv'), 'w') as csv_file: 
            csv_file.write(comparison_header)
            csv_writer = csv.writer(csv_file, delimiter=',') 
            #csv_writer.writerow(comparison_header)
            for compare in cls.list_compares:
                csv_writer.writerow([compare.filename, 
                                    compare.date,
                                    compare.num_etl,
                                    compare.num_src,
                                    compare.total_matched,
                                    compare.broken_attributes,
                                    compare.indentical])



    

def main(etalon_path, source_path):
    filesprepare = FileFolderPrepare(etalon_path, source_path)
    list_files = filesprepare.get_list_files()
    size_header = [5,3,2,3,2]
    #size_body = [6,5,5,14,10,10,21,21,21,31,31,17,10,10,5,10,5,10,3,10,10,1,1,1,1,10,20,20,10,10,5,5,5,15,15]
    
    size_body = { 'bisFirstLeg'          : 6,
                'recordType'            : 5, 
                'callDirection'         :5,
                'callTimeOut'           : 14,
                'callDuration'          : 10,
                'dataVolume'            : 10,
                'id'                    : 21,
                'id2'                   :  21,
                'id3'                   : 21,
                'otherParty'            : 31,
                'fwdA'                  : 31,
                'imei'                  : 17,
                'incomingRoute'         : 10,
                'outgoingRoute'         : 10,
                'locationArea'          : 5,
                'cell'                  : 10,
                'olServiceType'         : 5,
                'olServiceId'           : 10,
                'olServiceExtension'    : 3,
                'serviceCode'           : 10,
                'chainReference'        : 10,
                'isForwarded'           : 1,
                'isPreCharged'          : 1,
                'isHasTax'              : 1,
                'isFirstCdr'            : 1,
                'companyZone'           : 10,
                'charge'                : 20,
                'mscId'                 : 20,
                'seqN'                  : 10,
                'originNetworkId'       : 10,
                'rnA'                   : 5,
                'rnB'                   : 5,
                'rnC'                   : 5,
                'resourceParamType'     : 15,
                'resourceParamValue'    : 15,
            }
    size_trailer = [5,3,2,3,2]
    for filename in list_files:
        etl = GetRecordsFile(filesprepare.path_etl, filename)
        src = GetRecordsFile(filesprepare.path_src, filename)
        factory = FileComparator(size_header, size_body, size_trailer)
        comparer = factory.create_comparer(etl.records, src.records, filename, full=True)
        comparer.compare(list(size_body.keys()))
        Report.list_compares.append(comparer)
    Report.to_excel('res')


main('etl', 'src')
        

