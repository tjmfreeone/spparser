from spparser import AsyncReader
import asyncio

count = 0

async def main():
    es_getter = AsyncReader.async_anyfile_reader("./test.py",batch_size=100, trim_each_line=True)
    async for items in es_getter:
        for item in items:
            print(item)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

