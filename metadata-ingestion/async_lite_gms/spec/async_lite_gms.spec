# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.building.api import PYZ, EXE, COLLECT
from PyInstaller.building.build_main import Analysis

a = Analysis(['..\\..\\..\\src\\zeta_lab\\async_lite_gms.py'],
             pathex=['C:\\git\\datahub\\metadata-ingestion','C:\\git\\datahub\\metadata-ingestion\\src\\datahub','C:\\git\\datahub\\metadata-ingestion\\src\\zeta_lab'],
             binaries=[],
             datas=[],
             hiddenimports=['zeta_lab.utilities', 'Crypto', 'Crypto.Cipher.AES', 'Crypto.Util.Padding', 'Crypto.Random'],
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
          name='async_lite_gms',
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
          icon='C:\\git\\datahub\\metadata-ingestion\\pyinstaller\\async_lite_gms\\spec\\async_lite_gms.ico')