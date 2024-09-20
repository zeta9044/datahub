# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.building.api import PYZ, EXE, COLLECT
from PyInstaller.building.build_main import Analysis
import os

# Define the paths
zeta_lab_path = 'C:\\git\\datahub\\metadata-ingestion\\src\\zeta_lab'
datahub_path = 'C:\\git\\datahub\\metadata-ingestion\\src\\datahub'

a = Analysis(['C:\\git\\datahub\\metadata-ingestion\\src\\zeta_lab\\ingest_cli.py'],
             pathex=[zeta_lab_path, datahub_path],
             binaries=[],
             datas=[
                 (zeta_lab_path, 'zeta_lab'),
                 (datahub_path, 'datahub')
             ],
             hiddenimports=[],
             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             noarchive=False)

pyz = PYZ(a.pure, a.zipped_data)

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
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True,
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None,
          icon='C:\\git\\datahub\\metadata-ingestion\\pyinstaller\\ingest_cli\\spec\\ingest_cli.ico')