import logging
import csv
import pymongo

from .utils import Exceptions
from hashlib import md5
from pymongo import UpdateOne


class BaseWriter(object):
    def __init__(self):
        logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d  %H:%M:%S')


class async_csv_writer(BaseWriter):
    def __init__(self, file_path, mode="w", newline=None, each_line_type="dict", headers=None, encoding="utf-8", debug=True, **kwargs):
        if each_line_type not in ["list","dict"]:
            raise Exceptions.ArgValueError("each_line_type must be list or dict")
        self.file_path = file_path
        self.mode = mode
        self.newline = newline
        self.each_line_type = each_line_type
        self.headers = headers
        self.encoding = encoding
        self.total_count = 0
        self.debug = debug
        self.f = open(file=file_path, mode=self.mode, encoding=self.encoding, newline=self.newline)
        if self.headers:
            self.has_headers = True
            if self.each_line_type == "list":
                self.writer = csv.writer(self.f, fieldnames=self.headers)
            elif self.each_line_type == "dict":
                self.writer = csv.DictWriter(self.f, fieldnames=self.headers)
                self.writer.writeheader()
        else:
            self.has_headers = False

    def __enter__(self):
        return self

    def __exit__(self,exc_type, exc_value, traceback):
        if self.debug:
            logging.info("to destination: {}, total write {} items.".format(self.file_path, self.total_count))

    def _get_headers(self,data):
        if self.each_line_type == "list":
            self.writer = csv.writer(self.f)
        elif self.each_line_type == "dict":
            self.writer = csv.DictWriter(self.f, fieldnames=list(data[0].keys()))
            self.writer.writeheader()

        self.has_headers = True

    async def write(self, data):
        if  not isinstance(data,list):
            raise Exceptions.ArgValueError("input data type must be list")

        if not self.has_headers:
            self._get_headers(data)
        
        for line in data:
            if not line:
                continue
            self.writer.writerow(line)
            self.total_count += 1
        if self.debug:
            logging.info("to destination: {}, write {} items.".format(self.file_path,len(data)))


class async_anyfile_writer(BaseWriter):
    def __init__(self, file_path, mode="w", newline=None, encoding="utf-8", debug=True, **kwargs):
        self.file_path = file_path
        self.mode = mode
        self.newline = newline
        self.encoding = encoding
        self.total_count = 0
        self.debug = debug
        self.f = open(file=file_path, mode=self.mode, encoding=self.encoding, newline=self.newline)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.debug:
            logging.info("to destination: {}, total write {} lines.".format(self.file_path, self.total_count))

    async def write(self, data):
        if not isinstance(data, list):
            raise Exceptions.ArgValueError("input data type must be list")
        for line in data:
            self.f.write(str(line)+'\n')
            self.total_count += 1
        if self.debug:
            logging.info("to destination: {}, write {} lines.".format(self.file_path,len(data)))


class async_mongo_writer(BaseWriter):
    def __init__(self, collection, host=None, port=None, database=None, username=None, password=None, debug=True, **kwargs):
        super().__init__()
        if not host or not port or not database:
            raise Exceptions.ParamsError("lack of mongodb's host or port or database")
        self.collection_name = collection
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.debug = debug
        self.finished = None
        self.total_count = 0
        self._init_client()
    
    def _init_client(self):
        self.client = pymongo.MongoClient(host=self.host, port=self.port)
        self.db = self.client[self.database]
        self.db.authenticate(name=self.username, password=self.password)
        self.collection = self.db[self.collection_name]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.debug:
            logging.info("to destination: {}.{}, total write {} lines.".format(self.database, self.collection.name, self.total_count))
    
    async def write(self, data):
        if not isinstance(data, list):
            raise Exceptions.ArgValueError("input data type must be list")
        for line in data:
            if "_id" not in line.keys():
                line["_id"] = md5(str(line).encode()).hexdigest()
        self.collection.bulk_write([UpdateOne({"_id":line["_id"]}, {"$set":line}, upsert=True) for line in data])
        self.total_count += len(data)
        if self.debug:
            logging.info("to destination: {}.{}, write {} lines.".format(self.database, self.collection.name, len(data)))
