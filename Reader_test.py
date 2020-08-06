from spparser import Reader,Writer, AsyncReader, AsyncWriter
import asyncio
from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig
import time

async def main():
    sql = "CREATE TABLE IF NOT EXISTS BS (_id VARCHAR(32),appCode VARCHAR(1032),catId1 VARCHAR(1048),catId2 VARCHAR(1048),catName1 VARCHAR(1028),catName2 VARCHAR(1028),coverUrl VARCHAR(1106),description VARCHAR(1037),id VARCHAR(1048),marketPrice DOUBLE,monthSaleCount DOUBLE,price DOUBLE,promotions VARCHAR(1035),referId VARCHAR(1048),saleCount DOUBLE,sellerId VARCHAR(1048),soldout VARCHAR(1029),sortId DOUBLE,sortIdbyCat DOUBLE,stockSize DOUBLE,title VARCHAR(1040),vipPrice DOUBLE) DEFAULT CHARSET=utf8;"
    getter = AsyncReader.async_mysql_reader(query_sql="select * from ES",host="localhost", port=None, database="test", username="admin", password="12qwaszx",batch_size=100,max_read_lines=1000)
    with AsyncWriter.async_mysql_writer(create_table_sql=sql,host="localhost", port=None, database="test", username="admin", password="12qwaszx") as writer:
        async for items in getter:
            await writer.write(items)
        #for item in items:
        #    print(item)
                # Parser process

async def idata():
    rconfig =  GetterConfig.RMongoConfig("xiaohongshu_post_id_update_to_20200719",host='120.79.97.220',port='55555',username='jinmin',password='jinmin',database='jinmin_data',per_limit=100,max_limit=1000,max_retry=10)
    getter = ProcessFactory.create_getter(rconfig)
    wconfig = WriterConfig.WMongoConfig("test2",host='120.79.97.220',port='55555',username='jinmin',password='jinmin',database='jinmin_data',per_limit=100,max_limit=1000,max_retry=10)
    with ProcessFactory.create_writer(wconfig) as writer:
        async for items in getter:
            await writer.write(items)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    ts1 = time.time()
    loop.run_until_complete(main())
    t1 = time.time() - ts1

    #ts2 = time.time()
    #loop.run_until_complete(idata())
    #t2 = time.time() - ts2

    #print(t1, t2)
