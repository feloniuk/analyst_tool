import io
import time

import pandas as pd
import pyarrow as pa
from celery import Celery

from src import settings

client = Celery('app', broker=settings.REDIS_URL)
client.conf.result_backend = settings.REDIS_URL
context = pa.default_serialization_context()


@client.task
def analyse_data_set(file_path):
    time.sleep(10)
    df = pd.read_csv(file_path)
    buffer = io.StringIO()
    df.info(buf=buffer)
    data_frame_info = buffer.getvalue()
    return {
        'describe': df.describe().to_dict(),
        'info': data_frame_info,
    }
