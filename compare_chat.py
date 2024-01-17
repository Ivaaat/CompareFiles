import concurrent.futures
import json
import xlsxwriter

class FileComparator:
    def __init__(self, header_field_sizes, content_field_sizes, footer_field_sizes):
        self.header_field_sizes = header_field_sizes
        self.content_field_sizes = content_field_sizes
        self.footer_field_sizes = footer_field_sizes
        self.observers = []

    def compare_files(self, file1_path, file2_path):
        with open(file1_path, "r") as file1, open(file2_path, "r") as file2:
            header1, content1, footer1 = self._read_file(file1)
            header2, content2, footer2 = self._read_file(file2)

            if header1 != header2:
                self._notify_observers("Headers are not equal")
                return False, "Headers are not equal"
            if footer1 != footer2:
                self._notify_observers("Footers are not equal")
                return False, "Footers are not equal"
            if len(content1) != len(content2):
                self._notify_observers("Content lengths are not equal")
                return False, "Content lengths are not equal"

            for i in range(len(content1)):
                if content1[i] != content2[i]:
                    message = f"Content is not equal at line {i+1} and field(s): "
                    for j in range(len(content1[i])):
                        if content1[i][j] != content2[i][j]:
                            message += f"{j+1}, "
                    message = message[:-2]
                    self._notify_observers(message)
                    return False, message

            self._notify_observers("Files are equal")
            return True, "Files are equal"

    def _read_file(self, file):
        header = self._read_lines(file, self.header_field_sizes)
        content = self._read_lines(file, self.content_field_sizes)
        footer = self._read_lines(file, self.footer_field_sizes)
        return header, content, footer

    def _read_lines(self, file, field_sizes):
        lines = []
        for line_num, line in enumerate(file, start=1):
            fields = []
            start = 0
            for field_num, size in enumerate(field_sizes, start=1):
                field = line[start:start+size]
                fields.append(field)
                start += size
            lines.append(fields)
        return lines

    def add_observer(self, observer):
        self.observers.append(observer)

    def remove_observer(self, observer):
        self.observers.remove(observer)

    def _notify_observers(self, message):
        for observer in self.observers:
            observer.update(message)

class FileComparatorLogger:
    def __init__(self, file_path):
        self.file_path = file_path

    def update(self, message):
        with open(self.file_path, "a") as file:
            file.write(message + "\n")

class ExcelReportGenerator:
    def __init__(self, file_path, header_format, content_format, footer_format):
        self.file_path = file_path
        self.header_format = header_format
        self.content_format = content_format
        self.footer_format = footer_format
        self.workbook = xlsxwriter.Workbook(self.file_path)
        self.worksheet = self.workbook.add_worksheet()

    def generate_report(self, header, content, footer):
        self._write_lines(header, self.header_format)
        self._write_lines(content, self.content_format)
        self._write_lines(footer, self.footer_format)
        self.workbook.close()

    def _write_lines(self, lines, cell_format):
        row = 0
        for line in lines:
            col = 0
            for field in line:
                self.worksheet.write(row, col, field, cell_format)
                col += 1
            row += 1

class FileComparatorService:
    def __init__(self, comparator, logger, report_generator):
        self.comparator = comparator
        self.logger = logger
        self.report_generator = report_generator

    def compare_files(self, file1_path, file2_path):
        result, message = self.comparator.compare_files(file1_path, file2_path)
        self.logger.update(message)
        return result, message

    def generate_report(self, header, content, footer):
        self.report_generator.generate_report(header, content, footer)

class SOLIDFileComparator:
    def __init__(self, header_field_sizes, content_field_sizes, footer_field_sizes, logger, report_generator):
        self.comparator = FileComparator(header_field_sizes, content_field_sizes, footer_field_sizes)