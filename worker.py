import io
import pandas as pd
from celery import Celery
import settings

client = Celery('app', broker=settings.REDIS_URL)
client.conf.result_backend = settings.REDIS_URL


@client.task
def analyse_data_set(file_path):
    df = pd.read_csv(file_path)
    buffer = io.StringIO()
    df.info(buf=buffer)
    data_frame_info = buffer.getvalue()

    return {
        # 'describe': df.describe(),
        'info': data_frame_info,
        # 'hist': 'random/path/to/image'
    }
