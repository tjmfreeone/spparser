import csv
import logging
from .utils import Exceptions


class BaseReader(object):
    def __init__(self):
        logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d  %H:%M:%S')


class async_csv_reader(BaseReader):
    def __init__(self, file_path, mode='r',batch_size=10, max_read_lines=None, encoding="utf-8", each_line_type="dict", debug=True, **kwargs):
        super().__init__()
        if each_line_type not in ["dict", "list"]:
            raise Exceptions.ArgValueError("each_line_type must be dict or list")
        self.file_path = file_path
        self.max_read_lines = max_read_lines
        self.batch_size = batch_size
        self.mode = mode
        self.encoding = encoding
        self.debug = debug
        self.finished = False
        self.each_list = list()
        self.total_count = 0
        self.f = open(file=self.file_path, mode=self.mode, encoding=self.encoding)
        if each_line_type == "dict":
            self.reader = csv.DictReader(self.f)
        elif each_line_type == "list":
            self.reader = csv.reader(self.f)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.finished:
            if self.debug:
                logging.info("from source: {}, total get {} items.".format(self.file_path, self.total_count))
            self._reinit_vals()
            raise StopAsyncIteration

        for line in self.reader:
            if self.max_read_lines and self.total_count >= self.max_read_lines:
                self.finished = True
                break
            self.each_list.append(line)
            self.total_count += 1
            if len(self.each_list) >= self.batch_size:
                return self._ret_each()

        if self.each_list:
            self.finished = True
            return self._ret_each()
        if self.debug:
            logging.info("from source: {}, total get {} items.".format(self.file_path, self.total_count))
        self._reinit_vals()
        raise StopAsyncIteration

    def _reinit_vals(self):
        self.finished = False
        self.each_list = []
        self.total_count = 0
        self.f.seek(0, 0)
        self.read_lines_count = 0
        self.each_list.clear()

    def _ret_each(self):
        if self.debug and self.each_list:
            logging.info("from source: {}, this batch get {} items".format(self.file_path, len(self.each_list)))
        each = self.each_list
        self.each_list = list()
        return each


class async_anyfile_reader(BaseReader):
    def __init__(self,file_path, mode='r',batch_size=10, max_read_lines=None, encoding="utf-8", debug=True, **kwargs):
        super().__init__()
        self.file_path = file_path
        self.mode='r'
        self.batch_size = batch_size
        self.max_read_lines = max_read_lines
        self.encoding = encoding
        self.each_list = []
        self.f = open(self.file_path, mode=self.mode, encoding=self.encoding)
        self.debug = debug
        self.finished = None
        self.total_count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.finished:
            if self.debug:
                logging.info("from source: {}, total get {} lines.".format(self.file_path, self.total_count))
            self._reinit_vals()
            raise StopAsyncIteration

        for i in range(0,self.batch_size):
            if self.max_read_lines and self.total_count >= self.max_read_lines:
                self.finished = True
                break
            self.each_list.append(self.f.readline().strip())
            self.total_count += 1
            if len(self.each_list) >= self.batch_size:
                return self._ret_each()

        if self.each_list:
            self.finished = True
            return self._ret_each()
        if self.debug:
            logging.info("from source: {}, total get {} lines.".format(self.file_path, self.total_count))
        self._reinit_vals()
        raise StopAsyncIteration

    def _reinit_vals(self):
        self.finished = False
        self.each_list = []
        self.total_count = 0
        self.f.seek(0, 0)
        self.read_lines_count = 0
        self.each_list.clear()

    def _ret_each(self):
        if self.debug and self.each_list:
            logging.info("from source: {}, this batch get {} lines".format(self.file_path, len(self.each_list)))
        each = self.each_list
        self.each_list = list()
        return each

