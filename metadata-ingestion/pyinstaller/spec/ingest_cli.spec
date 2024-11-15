# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.building.api import PYZ, EXE, COLLECT
from PyInstaller.building.build_main import Analysis
import os
import sys

# 플랫폼 감지
is_windows = sys.platform.startswith('win')

# 현재 작업 디렉토리를 기준으로 경로 설정
base_path = os.getcwd()

# 가능한 경로들을 리스트로 정의
possible_paths = [
    os.path.join(base_path, 'src', 'zeta_lab'),
    os.path.join(base_path, 'metadata-ingestion', 'src', 'zeta_lab'),
    os.path.join(base_path, '..', 'src', 'zeta_lab'),
    os.path.join(base_path, '..', '..', 'src', 'zeta_lab'),
]

# zeta_lab_path 찾기
zeta_lab_path = next((path for path in possible_paths if os.path.exists(path)), None)

if zeta_lab_path is None:
    raise FileNotFoundError("zeta_lab 디렉토리를 찾을 수 없습니다.")
print('zeta_lab_path:',zeta_lab_path)
datahub_path = os.path.join(os.path.dirname(zeta_lab_path), 'datahub')
print('datahub_path:',datahub_path)

# 메인 스크립트 경로
main_script = os.path.join(zeta_lab_path, 'ingest_cli.py')

if not os.path.exists(main_script):
    raise FileNotFoundError(f"메인 스크립트를 찾을 수 없습니다: {main_script}")

a = Analysis([main_script],
             pathex=[zeta_lab_path, datahub_path],
             binaries=[],
             datas=[
                 (zeta_lab_path, 'zeta_lab'),
                 (datahub_path, 'datahub')
             ],
             hiddenimports=[
                 # 기존 imports에 추가
                 'psycopg2',
                 'psycopg2._psycopg',
                 'psycopg2.extensions',
                 'psycopg2.extras',
                 'psycopg2.pool',
                 'psycopg2.sql',

                 # SQLAlchemy 관련
                 'sqlalchemy',
                 'sqlalchemy.orm',
                 'sqlalchemy.ext.declarative',
                 'sqlalchemy.dialects.postgresql',

                # 기존 imports에 추가
                 'threading',
                 'threading.local',
                 '_thread',
                 'concurrent',
                 'concurrent.futures',
                 'concurrent.futures.thread',
                 'concurrent.futures.process',
                 'queue',
                 '_queue',

                 # async/await 관련
                 'asyncio.base_events',
                 'asyncio.proactor_events',
                 'asyncio.windows_events',
                 'asyncio.unix_events',
                 'asyncio.selector_events',
                 'asyncio.runners',

                 # DataHub 관련
                 'datahub.emitter.rest_emitter',
                 'datahub.configuration.common',
                 'datahub.metadata.schema_classes',
                 'datahub.utilities.server_config_util',

                 # HTTP/웹 서버 핵심 모듈
                 'httptools.parser.parser',
                 'httptools.parser.url_parser',
                 'multidict._multidict',
                 'websockets.speedups',
                 'uvloop',

                 # Uvicorn 관련 모듈
                 'uvicorn.logging',
                 'uvicorn.protocols.http.h11_impl',
                 'uvicorn.protocols.http.httptools_impl',
                 'uvicorn.protocols.websockets.websockets_impl',
                 'uvicorn.protocols.websockets.wsproto_impl',
                 'uvicorn.lifespan.on',
                 'uvicorn.lifespan.off',

                 # REST/HTTP 클라이언트 관련
                 'aiohttp',
                 'aiohttp.client',
                 'aiohttp.client_proto',
                 'aiohttp.client_reqrep',
                 'aiohttp.http_parser',
                 'aiohttp.http_writer',
                 'aiohttp.helpers',
                 'multidict._multidict',
                 'yarl._quoting_c',
                 'charset_normalizer',
                 'frozenlist._frozenlist',

                 # 비동기/동시성 관련
                 'concurrent.futures',
                 'concurrent.futures.thread',
                 'concurrent.futures.process',
                 '_asyncio',
                 'asyncio.base_events',
                 'asyncio.base_futures',
                 'asyncio.base_tasks',
                 'asyncio.events',
                 'asyncio.futures',
                 'asyncio.locks',
                 'asyncio.protocols',
                 'asyncio.queues',
                 'asyncio.runners',
                 'asyncio.streams',
                 'asyncio.subprocess',
                 'asyncio.tasks',
                 'asyncio.threads',
                 'asyncio.unix_events',

                 # DataHub 관련
                 'datahub.emitter.mcp',
                 'datahub.emitter.mcp_builder',
                 'datahub.emitter.rest_emitter',
                 'datahub.utilities.partition_executor',

                 # 기타 유틸리티
                 'contextlib',
                 'functools',
                 'threading',
                 'uuid',
                 '_json',
                 '_decimal'
             ],
             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             noarchive=False)

# 필요한 시스템 라이브러리 추가
import sysconfig
lib_dir = sysconfig.get_config_var('LIBDIR')
print("lib_dir:"+lib_dir)
if lib_dir:
    for lib in ['libm.so.6', 'libc.so.6', 'libpthread.so.0']:
        lib_path = os.path.join(lib_dir, lib)
        if os.path.exists(lib_path):
            a.binaries.append((lib, lib_path, 'BINARY'))

# 압축 없이 PYZ 생성
pyz = PYZ(a.pure, a.zipped_data, compress=False)

# 아이콘 경로 설정 (Windows에서만 사용)
icon_path = os.path.join(base_path, 'pyinstaller','spec', 'ingest_cli.ico') if is_windows else None

exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='ingest_cli',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=False,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True,
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None,
          icon=icon_path)

print(f"Base path: {base_path}")
print(f"Zeta lab path: {zeta_lab_path}")
print(f"Datahub path: {datahub_path}")
print(f"Main script path: {main_script}")