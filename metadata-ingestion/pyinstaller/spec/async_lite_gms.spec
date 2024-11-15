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
main_script = os.path.join(zeta_lab_path, 'async_lite_gms.py')

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
icon_path = os.path.join(base_path, 'pyinstaller', 'spec', 'async_lite_gms.ico') if is_windows else None

exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='async_lite_gms',
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
