from .utils import Exceptions
import csv
import re
import logging
import motor.motor_asyncio
import aiomysql

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
    def __init__(self,file_path, mode='r',batch_size=10, max_read_lines=None, encoding="utf-8", debug=True,trim_each_line=False, **kwargs):
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
        self.trim_each_line = trim_each_line

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

            line = self.f.readline()
            if line and self.trim_each_line:
                self.each_list.append(line.strip())
            elif line and not self.trim_each_line:
                self.each_list.append(line)
            else:
                self.finished = True
                break

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
    def __init__(self, collection, query=None, host=None, port=None, database=None, username=None, password=None, batch_size=10, max_read_lines=None, debug=True, **kwargs):
        super().__init__()
        if not host or not port or not database:
            raise Exceptions.ParamsError("lack of mongodb's host or port or database")
        self.collection_name = collection
        self.batch_size = batch_size
        self.query = query or {}
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.each_list = []
        self.debug = debug
        self.finished = False
        self.init_flag = False
        self.max_read_lines = max_read_lines
        
    async def _init_client(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
        self.db = self.client[self.database]
        self.collection = self.db[self.collection_name]
        self.cursor = self.collection.find(self.query, batch_size=self.batch_size)
        if self.query:
            self.docs_count = await self.collection.count_documents(self.query)
        else:
            self.docs_count = await self.collection.estimated_document_count()
        self.max_read_lines = min(self.max_read_lines, self.docs_count) if self.max_read_lines else self.docs_count
        self.percentage = None
        self.done_lines_num = 0


    def get_db(self):
        return self.db

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.init_flag:
            await self._init_client()
            self.init_flag = True

        if self.finished:
            if self.debug:
                logging.info("from source: {}.{}, total get {} lines.".format(self.database, self.collection.name, self.done_lines_num))
            self._reinit_vals()
            raise StopAsyncIteration

        async for line in self.cursor:
            if self.max_read_lines and self.done_lines_num >= self.max_read_lines:
                self.finished = True
                break
            self.done_lines_num += 1
            self.each_list.append(line)
            if len(self.each_list) >= self.batch_size:
                return self._ret_each()

        if self.each_list:
            self.finished = True
            return self._ret_each()

        if self.debug:
            logging.info("from source: {}.{}, total get {} lines.".format(self.database, self.collection.name, self.done_lines_num))
        self._reinit_vals()
        raise StopAsyncIteration 

    def _reinit_vals(self):
        self.finished = False
        self.each_list = []
        self.done_lines_num = 0
        self.each_list.clear()
        

    def _ret_each(self):
        if self.debug and self.each_list:
            logging.info("from source: {}.{}, this batch get {} lines, percentage: {:.2f}%".format(self.database, self.collection.name, len(self.each_list), self.done_lines_num/self.max_read_lines*100))
        each = self.each_list
        self.each_list = list()
        return each


class async_mysql_reader(BaseReader):
    def __init__(self, query_sql=None, host=None, port=None, database=None, username=None, password=None, charset='utf8', batch_size=10, max_read_lines=None,debug=True, **kwargs):
        super().__init__()
        if not host or not database:
            raise Exceptions.ParamsError("lack of mysql's host or database")
        if not query_sql:
            raise Exceptions.ParamsError("lack of query_sql")

        self.batch_size = batch_size
        self.query_sql = query_sql 
        self.table_name = self._get_table_name()
        self.host = host
        self.port = port or 3306
        self.username = username
        self.password = password
        self.database = database
        self.charset = charset
        self.each_list = []
        self.debug = debug
        self.finished = False
        self.done_lines_num = 0
        self.percentage = None
        self.max_read_lines = max_read_lines
        self._init_flag = False

    async def _init_connection(self):
        self.conn = await aiomysql.connect(host=self.host, port=self.port, user=self.username, 
                                           password=self.password, db=self.database, charset=self.charset)
        self.cursor = await self.conn.cursor(aiomysql.cursors.DictCursor)
        self.docs_count = await self._get_query_lines_count()
        self.max_read_lines = min(self.max_read_lines, self.docs_count) if self.max_read_lines else self.docs_count
        await self.cursor.execute(self.query_sql)

    def _get_table_name(self):
        flag = False
        for word in self.query_sql.split(" "):
            if word.upper() == "FROM":
                flag = True
                continue
            if flag and word:
                return word
            
    async def _get_query_lines_count(self):
        target = re.search(r"SELECT(.*?)FROM", self.query_sql, re.I).group(1).strip()
        await self.cursor.execute("SELECT COUNT({}) FROM {}".format(target, self.table_name))
        count = await self.cursor.fetchone()
        return count["COUNT({})".format(target)]
        
    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._init_flag:
            await self._init_connection()
            self._init_flag = True

        if self.finished:
            if self.debug:
                logging.info("from source: {}.{}, total get {} lines.".format(self.database, self.table_name, self.done_lines_num))
            await self._reinit_vals_and_close()
            raise StopAsyncIteration
        
        while True:
            line = await self.cursor.fetchone()
            if self.max_read_lines and self.done_lines_num >= self.max_read_lines:
                self.finished = True
                break
            self.done_lines_num += 1
            self.each_list.append(line)
            if len(self.each_list) >= self.batch_size:
                return self._ret_each()

        if self.each_list:
            self.finished = True
            return self._ret_each()

        if self.debug:
            logging.info("from source: {}.{}, total get {} lines.".format(self.database, self.table_name, self.done_lines_num))
        await self._reinit_vals_and_close()
        raise StopAsyncIteration
        
    async def _reinit_vals_and_close(self):
        self.finished = False
        self.each_list.clear()
        await self.cursor.close()
        self.conn.close()

    def _ret_each(self):
        if self.debug and self.each_list:
            logging.info("from source: {}.{}, this batch get {} lines, percentage: {:.2f}%".format(self.database, self.table_name, len(self.each_list), self.done_lines_num/self.max_read_lines*100))
        each = self.each_list
        self.each_list = list()
        return each
