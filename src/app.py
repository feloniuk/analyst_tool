import os
from pathlib import Path
from urllib.parse import urlparse
import aiohttp_cors
from celery.states import SUCCESS, PENDING

import aiofiles
import aiohttp
import aiohttp_jinja2
import aioredis
import jinja2
import pandas as pd
from aiohttp import web
from aiohttp_session import get_session
from aiohttp_session import session_middleware
from aiohttp_session.redis_storage import RedisStorage
from celery.result import AsyncResult

from src import settings
from src.worker import analyse_data_set, client

routes = web.RouteTableDef()


@routes.get('/analyse/start')
@aiohttp_jinja2.template('analyse_start.html')
async def upload_form(request):
    return {'title': 'Welcome Page'}


async def update_cookie(request, file):
    session = await get_session(request)
    if 'files' in session:
        session['files'] = session['files'] + file
    else:
        session['files'] = file


@routes.post('/analyse/start')
@aiohttp_jinja2.template('analyse_succeed_created.html')
async def upload_process(request):
    async for obj in (await request.multipart()):
        if obj.filename is not None:
            file_path = os.path.join(settings.MEDIA_ROOT, obj.filename)
            f = await aiofiles.open(file_path, 'wb')
            await f.write(await obj.read())
            await f.close()
            task_id = analyse_data_set.delay(file_path).id
            await update_cookie(request, [
                {
                    'file_name': obj.filename,
                    'task_id': task_id
                }
            ])


@routes.get(r'/analyse/list')
@aiohttp_jinja2.template('analyse_list.html')
async def get_analyse_list(request):
    session = await get_session(request)
    return {'analysed_list': session.get('files')}


@routes.get(r'/analyse/status/{task_id}')
async def get_task_status(request):
    task_id = request.match_info['task_id']
    status = AsyncResult(task_id, app=client).status
    return web.json_response(
        {
            'status': status,
            'task_id': task_id
        }
    )


@routes.get(r'/analyse/result/{task_id}')
@aiohttp_jinja2.template('analyse_result.html')
async def get_result_view(request):
    result = AsyncResult(request.match_info['task_id'], app=client).get()
    return {
        'describe': pd.DataFrame.from_dict(result['describe']).to_html(),
        'info': result['info'],
    }


async def init():
    redis_connection_info = urlparse(settings.REDIS_URL)
    redis = await aioredis.create_pool((
        redis_connection_info.hostname,
        redis_connection_info.port
    ))
    app = web.Application(middlewares=[session_middleware(RedisStorage(redis))])
    app.add_routes(routes)
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
    })
    for route in list(app.router.routes()):
        cors.add(route)
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(settings.TEMPLATE_ROOT))
    Path(settings.MEDIA_ROOT).mkdir(parents=True, exist_ok=True)
    return app


web.run_app(
    init(),
    port=settings.WEB_PORT,
    host=settings.WEB_HOST
)
