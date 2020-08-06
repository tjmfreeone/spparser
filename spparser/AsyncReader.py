from .utils import Exceptions
import csv
import re
import logging
import pymongo
import pymysql

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
                logging.info("from source: {}, total get {} lines.".format(self.file_path, self.total_count))
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
        self.finished = False
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
        self.finished = False
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
                logging.info("from source: {}.{}, total get {} lines.".format(self.database, self.collection.name, self.total_count))
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
            logging.info("from source: {}.{}, total get {} lines.".format(self.database, self.collection.name, self.total_count))
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


class async_mysql_reader(BaseReader):
    def __init__(self, query_sql=None, host=None, port=None, database=None, username=None, password=None,charset='utf8', batch_size=10, max_read_lines=None,debug=True, **kwargs):
        super().__init__()
        if not host or not database:
            raise Exceptions.ParamsError("lack of mysql's host or database")
        if not query_sql:
            raise Exceptions.ParamsError("lack of query_sql")

        self.batch_size = batch_size
        self.query_sql = query_sql 
        self.table_name = self._get_table_name()
        self.host = host
        self.port = port if port else 3306
        self.username = username
        self.password = password
        self.database = database
        self.charset = charset
        self.each_list = []
        self.debug = debug
        self.finished = False
        self.total_count = 0
        self._init_connection()
        self.max_read_lines = max_read_lines if max_read_lines else self._get_query_lines_count()
        self.cursor.execute(self.query_sql)
        self.percentage = None
        self.done_lines_num = 0

    def _init_connection(self):
        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.username, password=self.password, 
                database=self.database, charset=self.charset)
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

    def get_connection(self):
        '''
        return new connection
        '''
        return pymsql.connect(host=self.host, port=self.port, user=self.username, password=self.password,
                database=self.database, charset=self.charset)

    def _get_table_name(self):
        flag = False
        for word in self.query_sql.split(" "):
            if word.upper() == "FROM":
                flag = True
                continue
            if flag and word:
                return word
            
    def _get_query_lines_count(self):
        target = re.search(r"SELECT(.*?)FROM",self.query_sql, re.I).group(1).strip()
        self.cursor.execute("SELECT COUNT({}) FROM {}".format(target, self.table_name))
        return self.cursor.fetchone()["COUNT({})".format(target)]
        
    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.finished:
            if self.debug:
                logging.info("from source: {}.{}, total get {} lines.".format(self.database, self.table_name, self.total_count))
            self._reinit_vals_and_close()
            raise StopAsyncIteration
        
        while True:
            line = self.cursor.fetchone()
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
            logging.info("from source: {}.{}, total get {} lines.".format(self.database, self.table_name, self.total_count))
        self._reinit_vals_and_close()
        raise StopAsyncIteration
        
    def _reinit_vals_and_close(self):
        self.finished = False
        self.each_list = []
        self.total_count = 0
        self.read_lines_count = 0
        self.each_list.clear()
        self.cursor.close()
        self.conn.close()

    def _ret_each(self):
        if self.debug and self.each_list:
            logging.info("from source: {}.{}, this batch get {} lines, percentage: {:.2f}%".format(self.database, self.table_name, len(self.each_list), self.done_lines_num/self.max_read_lines*100))
        each = self.each_list
        self.each_list = list()
        return each
