import asyncio
import io
import pandas as pd


async def create_task(file_path):
    await asyncio.sleep(5)


# TODO: unmock get result method
async def get_result(task_id: str) -> dict:
    df = pd.read_csv('~/Downloads/test_data.csv')
    # await asyncio.sleep(1)
    buffer = io.StringIO()
    df.info(buf=buffer)
    s = buffer.getvalue()

    return {
        'describe': df.describe(),
        'info': s,
        'hist': 'random/path/to/image'
    }
