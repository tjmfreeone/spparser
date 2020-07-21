import csv
import logging
from .utils import Exceptions
import pymongo


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


class async_mongo_reader(BaseReader):
    def __init__(self, collection, query=None, host=None, port=None, database=None, username=None, password=None, batch_size=10, max_read_lines=None,debug=True, **kwargs):
        super().__init__()
        if not host or not port or not database:
            raise Exceptions.ParamsError("lack of mongodb's host or port or database")
        self.collection_name = collection
        self.batch_size = batch_size
        self.query = query if query else {}
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.each_list = []
        self.debug = debug
        self.finished = None
        self.total_count = 0
        self._init_client()
        self.max_read_lines = max_read_lines if max_read_lines else self.collection.find(self.query).count()
        self.percentage = None
        self.done_lines_num = 0

    def _init_client(self):
        self.client = pymongo.MongoClient(host=self.host, port=self.port)
        self.db = self.client[self.database]
        self.db.authenticate(name=self.username, password=self.password)
        self.collection = self.db[self.collection_name]
        self.cursor = self.collection.find(self.query, batch_size=self.batch_size)

    def get_db(self):
        return self.db

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.finished:
            if self.debug:
                logging.info("from source: {}.{}, total get {} items.".format(self.database, self.collection.name, self.total_count))
            self._reinit_vals()
            raise StopAsyncIteration

        for line in self.cursor:
            if self.max_read_lines and self.total_count >= self.max_read_lines:
                self.finished = True
                break
            self.done_lines_num += 1
            self.each_list.append(line)
            self.total_count += 1
            if len(self.each_list) >= self.batch_size:
                return self._ret_each()

        if self.each_list:
            self.finished = True
            return self._ret_each()
        if self.debug:
            logging.info("from source: {}.{}, total get {} items.".format(self.database, self.collection.name, self.total_count))
        self._reinit_vals()
        raise StopAsyncIteration
        

    def _reinit_vals(self):
        self.finished = False
        self.each_list = []
        self.total_count = 0
        self.read_lines_count = 0
        self.each_list.clear()
        #self.collection.cusor.close()

    def _ret_each(self):
        if self.debug and self.each_list:
            logging.info("from source: {}.{}, this batch get {} lines, percentage: {:.2f}%".format(self.database, self.collection.name, len(self.each_list), self.done_lines_num/self.max_read_lines*100))
        each = self.each_list
        self.each_list = list()
        return each

