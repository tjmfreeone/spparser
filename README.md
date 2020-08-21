[中文介绍](./README_CN.md)
# Introduction
The goal of spparser is to provide a concise and efficient way to read, write, and process text data. At the same time, it supports synchronous and asynchronous reading and writing files, and supports regular, xpath, css selector to extract data. In the future, read and write support for the database will be implemented, and NLP will be introduced to provide more flexible processing methods. The architecture diagram is as follows:  
![jiagou](https://github.com/taojinmin/MDimages/blob/master/spparser-images/jiagou-0.3.10.jpg)
 

The AsyncReader and AsyncWriter is inspired by @zpoint's [idataapi_transform](https://github.com/zpoint/idataapi-transform)



# Installation
```shell
pip3 install spparser
```

# Quick Start

```python
from spparser import Reader, Writer, Extractor

def main():
    data = Reader.read_csv(file_path="./example.csv", each_line_type="dict", max_read_lines=10)
    '''
    example.csv:
    field1,field2
    1,2
    3,4
    5,6
    '''
    '''
    read_csv result: data = [{'a': '122github', 'b': '2'}, {'a': '-8spparser999', 'b': '4'}]
    '''
    alist = []
    for item in data:
        res = Extractor.regex(r"[a-zA-Z]+", item["a"], flags=0, trim_mode=True, return_all=False)
        alist.append(res)
    '''
    alist = ["github","spparser"]
    '''
    Writer.write(alist, "result.json")

if __name__ == "__main__":
    main()
```
  
Use Extractor.xpath() to extract html text 
```python
from spparser import Reader, Writer, Extractor

def main():
    '''
    demo.html
    <html lang="en">
    <head>
        <title>spparser</title>
    </head>
    <body>
        <ul id="container">
            <li class="object-1" tag="1"/>
            <li class="object-2"/>
            <li class="object-3"/>
        </ul>
    </body>
    </html>
    '''
    '''
    read_csv result: data = [{'a': '122github', 'b': '2'}, {'a': '-8spparser999', 'b': '4'}]
    '''
    html_text = Reader.read_anyfile("demo.html",line_by_line=False)
    res = Extractor.xpath("//title/text()",html_text)
    print(res)

if __name__ == "__main__":
    main()
```  
Reading files asynchronously

```python
from spparser import Reader,Writer, AsyncReader, AsyncWriter
import asyncio

async def main():
    reader = AsyncReader.async_csv_reader("./src.csv",batch_size=10,each_line_type="dict",max_read_lines=100, debug=True)
    with AsyncWriter.async_csv_writer("./dest.csv") as writer:
        async for items in reader:
            #for item in items:
                # Parser process
            await writer.write(items)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
```
When debug is set to True, output logs:

```bash
[2020-07-17  14:54:04] AsyncReader.py[line:70] INFO: from source: ./src.csv, this batch get 10 lines
[2020-07-17  14:54:04] AsyncWriter.py[line:63] INFO: to destination: ./dest.csv, write 10 lines.
[2020-07-17  14:54:04] AsyncReader.py[line:70] INFO: from source: ./src.csv, this batch get 10 lines
[2020-07-17  14:54:04] AsyncWriter.py[line:63] INFO: to destination: ./dest.csv, write 10 lines.
[2020-07-17  14:54:04] AsyncReader.py[line:70] INFO: from source: ./src.csv, this batch get 10 lines
[2020-07-17  14:54:04] AsyncWriter.py[line:63] INFO: to destination: ./dest.csv, write 10 lines.
[2020-07-17  14:54:04] AsyncReader.py[line:70] INFO: from source: ./src.csv, this batch get 10 lines
[2020-07-17  14:54:04] AsyncWriter.py[line:63] INFO: to destination: ./dest.csv, write 10 lines.
...
```
For mongodb asynchronous read and write:
```python
async def main():
    reader = AsyncReader.async_mongo_reader(query={},collection="src_col", host="my_address",port=27017, database="my_db",username="my_name", password="my_pwd", batch_size=100,max_read_lines=1000)
    with AsyncWriter.async_mongo_writer(collection="dest_col", host="my_address",port=27017, database="my_db",username="my_name", password="my_pwd") as writer:
        async for items in getter:
            await writer.write(items)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
```
Version 0.4.10 added support for MySQL asynchronous read and write
```python
async def main():
    sql = "CREATE TABLE IF NOT EXISTS TARGET_TABLE (field1 type1, field2 type2) DEFAULT CHARSET=utf8;"
    getter = AsyncReader.async_mysql_reader(query_sql="SELECT * FROM SRC_TABLE",host="localhost", port=None, database="test", username="username", password="password",batch_size=100,max_read_lines=1000)
    with AsyncWriter.async_mysql_writer(create_table_sql=sql,host="localhost", port=None, database="test", username="username", password="password") as writer:
        async for items in getter:
            await writer.write(items)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
```
# History
## 0.2.10
- async_anyfile_reader, async_anyfile_writer, async_csv_reader, async_csv_writer support.
- xpath, css, regex selectors in Extractor support.
## 0.3.30
- async_mongo_reader, async_mongo_writer support
## 0.4.10
- async_mysql_reader, async_mysql_writer support
