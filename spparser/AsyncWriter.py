import logging
import csv
import pymongo
import pymysql

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
            logging.info("to destination: {}, total write {} lines.".format(self.file_path, self.total_count))

    def _get_headers(self,data):
        if self.each_line_type == "list":
            self.writer = csv.writer(self.f)
        elif self.each_line_type == "dict":
            self.writer = csv.DictWriter(self.f, fieldnames=list(data[0].keys()))
            self.writer.writeheader()

        self.has_headers = True

    async def write(self, data):
        if not data:
            if self.debug:
                logging.info("to destination: {}, write {} lines.".format(self.file_path,0))
            return 
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
            logging.info("to destination: {}, write {} lines.".format(self.file_path,len(data)))


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
        if not data:
            if self.debug:
                logging.info("to destination: {}, write {} lines.".format(self.file_path,0))
            return
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
        if not data:
            if self.debug:
                logging.info("to destination: {}.{}, write {} lines.".format(self.database, self.collection.name, len(data)))
            return
        if not isinstance(data, list):
            raise Exceptions.ArgValueError("input data type must be list")
        for line in data:
            if "_id" not in line.keys():
                line["_id"] = md5(str(line).encode()).hexdigest()
        self.collection.bulk_write([UpdateOne({"_id":line["_id"]}, {"$set":line}, upsert=True) for line in data])
        self.total_count += len(data)
        if self.debug:
            logging.info("to destination: {}.{}, write {} lines.".format(self.database, self.collection.name, len(data)))


class async_mysql_writer(BaseWriter):
    def __init__(self, table=None, host=None, port=None, database=None, username=None, password=None, charset='utf8', debug=True, create_table_sql=None, auto_id=False, **kwargs):
        super().__init__()
        if not host or not database or not host:
            raise Exceptions.ParamsError("lack of mysql's host or database or host")
        if table and create_table_sql:
            raise Exceptions.ParamsError("parameter table and create_table_sql cannot be set at the same time")
        if not table and not create_table_sql:
            raise Exceptions.ParamsError("one of  'table' and 'create_table_sql' must be set")
        self.create_table_sql = create_table_sql
        self.table_name = table if table else self._get_table_name()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.charset = charset
        self._init_connection()
        self.auto_id = auto_id
        self.debug = debug
        self.finished = None
        self.total_count = 0
        self.is_already_init_table = False
    
    def _init_connection(self):
        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.username, password=self.password,
                database=self.database, charset=self.charset)
        self.cursor = self.conn.cursor()

    def _init_table(self, data_0=None):
        if self.create_table_sql:
            self.cursor.execute(self.create_table_sql)
            self.is_already_init_table = True
            return
        if not data_0:
            self.is_already_init_table = True
            return

        field_config = ["_id VARCHAR(32)"] if self.auto_id and "_id" not in data_0 else []
        for k,v in data_0.items():
            if k == "_id":
                field_config.append("{} VARCHAR({})".format(k,len(v)))
                continue
            if type(v) in [str, list, dict, tuple, set, bool] or v is None:
                field_config.append("{} VARCHAR({})".format(k ,len(str(v))+1024))
                continue
            if type(v) in [int,float]:
                field_config.append("{} DOUBLE".format(k))
        if self.auto_id:
            field_config.append("PRIMARY KEY(_id)")

        init_sql = "CREATE TABLE IF NOT EXISTS {} ({}) DEFAULT CHARSET={};".format(self.table_name, ",".join(field_config), self.charset)
        self.cursor.execute(init_sql)
        self.is_already_init_tagble = True

    def _get_table_name(self):
        flag = False
        for word in self.create_table_sql.split(" "):
            if word.upper() == "TABLE":
                flag = True
                continue
            if word.upper() in ["IF", "NOT","EXISTS"]:
                continue
            if flag and word:
                return word

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.close()
        self.conn.close()
        if self.debug:
            logging.info("to destination: {}.{}, total write {} lines.".format(self.database, self.table_name, self.total_count))
    
    async def write(self, data):
        if not data:
            if self.debug:
                logging.info("to destination: {}.{}, write {} lines.".format(self.database, self.table_name, len(data)))
            return
        if not self.is_already_init_table:
            self._init_table(data_0=data[0])
        if not isinstance(data, list):
            raise Exceptions.ArgValueError("input data type must be list")

        try:
            for line in data:
                if self.auto_id and "_id" not in line.keys():
                    line["_id"] = md5(str(line).encode()).hexdigest() 
                values_config = []
                for v in line.values():
                    if type(v) in [str, list, dict, tuple, set, bool]:
                        values_config.append('"'+str(v)+'"')
                    if type(v) in [int,float]:
                        values_config.append(str(v))
                    if v is None:
                        values_config.append("null")
                replace_sql = "REPLACE INTO {}({})  VALUES ({});".format(self.table_name, ",".join(line.keys()), ",".join(values_config))
                self.cursor.execute(replace_sql)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exceptions.AsyncWriterError(e)
        
        self.total_count += len(data)
        if self.debug:
            logging.info("to destination: {}.{}, write {} lines.".format(self.database, self.table_name, len(data)))
