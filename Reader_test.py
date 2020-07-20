from spparser import Reader,Writer, AsyncReader, AsyncWriter
import asyncio


async def main():
    getter = AsyncReader.async_csv_reader("/root/jupyter_notebook/ele_watsons_goods_20191201.csv",batch_size=10,each_line_type="dict",max_read_lines=100)
    with AsyncWriter.async_csv_writer("./example.txt") as writer:
        async for items in getter:
            #for item in items:
                # Parser process
            await writer.write(items)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
