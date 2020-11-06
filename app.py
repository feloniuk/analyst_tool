import os
from pathlib import Path
import uuid
import io


import aiofiles
from celery.result import AsyncResult
import aiohttp_jinja2
import aioredis
import jinja2
from aiohttp import web
from aiohttp_session import get_session
from worker import analyse_data_set, client
from aiohttp_session import session_middleware
from aiohttp_session.redis_storage import RedisStorage

import settings

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
            f = await aiofiles.open(file_path,  'wb')
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


@routes.get(r'/analyse/result/{task_id}')
@aiohttp_jinja2.template('analyse_result.html')
async def get_result_view(request):
    result = AsyncResult(request.match_info['task_id'], app=client).get()
    return {
        # 'describe': df['describe'].to_html(),
        'info': result['info'],
    }


async def init():
    redis = await aioredis.create_pool(('localhost', 6379))
    app = web.Application(middlewares=[session_middleware(RedisStorage(redis))])
    app.add_routes(routes)
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(settings.TEMPLATE_ROOT))
    Path(settings.MEDIA_ROOT).mkdir(parents=True, exist_ok=True)
    return app

web.run_app(init(), port=8080)
