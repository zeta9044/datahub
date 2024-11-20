import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from typing import Dict, Any

import click

from datahub.ingestion.reporting.datahub_ingestion_run_summary_provider import DatahubIngestionRunSummaryProvider
from datahub.ingestion.reporting.file_reporter import FileReporter
from datahub.ingestion.reporting.reporting_provider_registry import reporting_provider_registry
from datahub.ingestion.source.sql_queries import SqlQueriesSource
from datahub.ingestion.sink.console import ConsoleSink
from datahub.ingestion.sink.datahub_lite import DataHubLiteSink
from datahub.ingestion.sink.datahub_rest import DatahubRestSink
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from zeta_lab.pipeline import ingest_metadata, make_sqlsrc, extract_lineage, move_lineage
from zeta_lab.source.sqlsrc_to_json_converter import SqlsrcToJSONConverter
from zeta_lab.source.convert_to_qtrack_db import ConvertQtrackSource
from zeta_lab.source.qtrack_meta_source import QtrackMetaSource
from zeta_lab.utilities.tool import get_server_pid

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL 중 선택)
    format="%(asctime)s - %(levelname)s - %(message)s",  # 포맷 설정
)
logger = logging.getLogger(__name__)

def safe_register(registry, name, class_):
    if name not in registry.mapping:
        registry.register(name, class_)
        logger.debug(f"Registered {name} in {registry.__class__.__name__}")
    else:
        logger.debug(f"{name} already registered in {registry.__class__.__name__}")

# Register sources
safe_register(source_registry, "sql-queries", SqlQueriesSource)
safe_register(source_registry, "sqlsrc-to-json-converter", SqlsrcToJSONConverter)
safe_register(source_registry, "convert-to-qtrack-db", ConvertQtrackSource)
safe_register(source_registry, "qtrack-meta-source", QtrackMetaSource)

# Register sinks
safe_register(sink_registry, "console", ConsoleSink)
safe_register(sink_registry, "datahub-lite", DataHubLiteSink)
safe_register(sink_registry, "datahub-rest", DatahubRestSink)

# Register reporters
safe_register(reporting_provider_registry, "datahub", DatahubIngestionRunSummaryProvider)
safe_register(reporting_provider_registry, "file", FileReporter)

# Log all registered components
def log_registered_components(registry, registry_name):
    logger.debug(f"Registered {registry_name}:")
    for name, class_ in registry.mapping.items():
        logger.debug(f"  {name} -> {class_}")

log_registered_components(source_registry, "sources")
log_registered_components(sink_registry, "sinks")
log_registered_components(reporting_provider_registry, "reporters")

# Global variables for configuration
config: Dict[str, Any] = {
    "log_file": "async_lite_gms.log",
    "db_file": "async_lite_gms.db",
    "log_level": "INFO",
    'port': 8000,
    'workers': 4,
    'batch_size': 1000,
    'cache_ttl': 300,
    'pid_file': 'async_lite_gms.pid'  # PID 파일 경로 추가
}


def get_base_path():
    """
    :return: Returns the base path of the application. If the application
        is running as a bundled executable, it returns the path to the
        bundled data. Otherwise, it returns the directory in which the
        current script resides.
    """
    if getattr(sys, 'frozen', False):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))


def find_config_file(base_path):
    """Find the configuration file."""
    possible_names = ['meta_config.json', 'config.json']
    search_paths = [
        os.environ.get('META_CONFIG_FILE'),  # Check environment variable for config file path
        base_path,
        os.getcwd(),  # Current working directory
        getattr(sys, '_MEIPASS', None),  # PyInstaller's temporary directory
    ]
    for path in search_paths:
        if path is None:
            continue
        if os.path.isfile(path):  # If the environment variable points directly to a file
            return path
        for name in possible_names:
            full_path = os.path.join(path, name)
            if os.path.exists(full_path):
                return full_path
    return None


def load_config(ctx, config_file=None):
    """
    :param ctx: Context object that holds configuration and other contextual data
    :param config_file: Optional path to the configuration file. If not provided, it will be determined based on the context or environment variables.
    :return: The context object with updated configuration settings
    """
    global config
    base_path = get_base_path()

    if config_file is None:
        config_file = ctx.obj.get('config_file') or os.environ.get('META_CONFIG_FILE')

    if config_file is None:
        config_file = find_config_file(base_path)

    if config_file and os.path.exists(config_file):
        with open(config_file, 'r') as f:
            loaded_config = json.load(f)
            if 'log_level' in loaded_config:
                loaded_config['log_level'] = loaded_config['log_level'].upper()
            config.update(loaded_config)
        logger.info(f"Loaded configuration from {config_file}")
    else:
        logger.info("Configuration file not found. Using default settings.")

    ctx.obj['config_file'] = config_file
    ctx.obj['config'] = config


def save_config(ctx):
    """
    :param ctx: Context object containing configuration details and environment settings.
    :return: None
    """
    config_file = ctx.obj.get('config_file')

    if config_file is None:
        config_file = os.environ.get('META_CONFIG_FILE')

    if config_file is None:
        config_file = find_config_file(get_base_path())

    if config_file is None:
        config_file = os.path.join(get_base_path(), 'meta_config.json')

    with open(config_file, 'w') as f:
        json.dump(ctx.obj['config'], f, indent=2)
    logger.info(f"Saved configuration to {config_file}")

@click.group()
@click.option('--config-file', type=click.Path(exists=True), help="Path to the configuration file")
@click.pass_context
def cli(ctx, config_file):
    """Ingestion CLI for managing the server and other operations."""
    ctx.ensure_object(dict)
    load_config(ctx, config_file)


@cli.group()
@click.pass_context
def gms(ctx):
    """Commands for managing the GMS server."""
    pass


def find_executable(base_path):
    """Find the appropriate executable or script."""
    possible_names = ['async_lite_gms', 'async_lite_gms.exe', 'async_lite_gms.py']
    search_paths = [
        os.environ.get('ASYNC_LITE_GMS_PATH'),  # 환경 변수에서 경로 확인
        base_path,
        os.getcwd(),  # 현재 작업 디렉토리
        getattr(sys, '_MEIPASS', None),  # PyInstaller의 임시 디렉토리
    ]
    for path in search_paths:
        if path is None:
            continue
        if os.path.isfile(path):  # 환경 변수가 직접 실행 파일을 가리키는 경우
            return path
        for name in possible_names:
            full_path = os.path.join(path, name)
            if os.path.exists(full_path):
                return full_path
    return None


@gms.command()
@click.pass_context
def start(ctx):
    """Start the GMS server."""
    pid = get_server_pid()
    if pid:
        click.echo(f"Server is already running (PID: {pid})")
        return

    base_path = get_base_path()
    exec_path = find_executable(base_path)

    logger.info(f"exec_path is {exec_path}")

    if not exec_path:
        click.echo("Error: async_lite_gms executable or script not found.")
        return

    cmd = [
        exec_path,
        "--log-file", ctx.obj['config']['log_file'],
        "--db-file", ctx.obj['config']['db_file'],
        "--log-level", ctx.obj['config']['log_level'],
        "--port", str(ctx.obj['config']['port']),
        "--workers", str(ctx.obj['config']['workers']),
        "--batch-size", str(ctx.obj['config']['batch_size']),
        "--cache-ttl", str(ctx.obj['config']['cache_ttl'])
    ]

    # 표준 출력을 로그 파일로 리디렉션
    stdout_log = open(ctx.obj['config']['log_file'], 'a')
    stderr_log = open(ctx.obj['config']['log_file'], 'a')  # 동일한 로그 파일에 stderr도 기록

    try:
        if sys.platform == 'win32':
            if exec_path.endswith('.py'):
                cmd.insert(0, sys.executable)
            process = subprocess.Popen(
                cmd,
                creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,  # 완전 분리
                stdout=stdout_log,
                stderr=stderr_log,
                close_fds=True
            )
        else:
            if exec_path.endswith('.py'):
                cmd.insert(0, sys.executable)
            process = subprocess.Popen(
                cmd,
                preexec_fn=os.setsid,  # 새로운 세션에서 실행
                stdout=stdout_log,
                stderr=stderr_log,
                close_fds=True
            )

        # PID 파일 생성
        pid_file_path = ctx.obj['config']['pid_file']
        with open(pid_file_path, 'w') as pid_file:
            pid_file.write(str(process.pid))
        logger.info(f"Server PID {process.pid} written to {pid_file_path}")

        # 서버 시작 대기 및 상태 체크
        max_retries = 30  # 최대 30초 대기
        server_ready = False

        for i in range(max_retries):
            logger.info(f"Server is starting... {i+1} s")
            time.sleep(1)

            # PID 파일에서 PID 읽기
            current_pid = get_server_pid_from_file(pid_file_path)
            if current_pid is None:
                continue

            # 프로세스 상태 확인
            if not is_process_running(current_pid):
                # 프로세스가 종료됨
                click.echo("Server process has terminated unexpectedly.")
                logger.error("Server process has terminated unexpectedly.")
                stdout_log.close()
                stderr_log.close()
                return

            # 서버 헬스 체크
            try:
                import requests
                response = requests.get(f"http://localhost:{ctx.obj['config']['port']}/health")
                if response.status_code == 200:
                    server_ready = True
                    break
            except requests.RequestException:
                continue

        if server_ready:
            click.echo("Server started successfully and is ready to accept requests.")
            logger.info("Server started successfully and is ready to accept requests.")
        else:
            click.echo("Server started but failed to respond to health check.")
            logger.error("Server started but failed to respond to health check.")
            # 서버 프로세스를 종료
            try:
                if sys.platform == 'win32':
                    subprocess.run(['taskkill', '/F', '/PID', str(process.pid)], check=True)
                else:
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                click.echo("Server process has been terminated due to failed health check.")
                logger.info("Server process has been terminated due to failed health check.")
            except Exception as e:
                click.echo(f"Failed to terminate server process: {str(e)}")
                logger.error(f"Failed to terminate server process: {str(e)}")
    except Exception as e:
        click.echo(f"Error starting server: {str(e)}")
        logger.error(f"Error starting server: {str(e)}")
    finally:
        stdout_log.close()
        stderr_log.close()


def get_server_pid_from_file(pid_file_path):
    """Read the server PID from the PID file."""
    try:
        with open(pid_file_path, 'r') as pid_file:
            pid = int(pid_file.read().strip())
            return pid
    except (FileNotFoundError, ValueError):
        return None


def is_process_running(pid):
    """Check if a process with the given PID is running."""
    try:
        if sys.platform == 'win32':
            # On Windows, use ctypes to check process existence
            import ctypes
            PROCESS_QUERY_INFORMATION = 0x0400
            process = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, False, pid)
            if process:
                ctypes.windll.kernel32.CloseHandle(process)
                return True
            else:
                return False
        else:
            # On Unix, sending signal 0 does not kill the process, but raises an error if the process does not exist
            os.kill(pid, 0)
            return True
    except OSError:
        return False


@gms.command()
@click.pass_context
def stop(ctx):
    """Stop the GMS server."""
    pid = get_server_pid()
    if not pid:
        click.echo("Server is not running.")
        return

    try:
        if sys.platform == 'win32':
            subprocess.run(['taskkill', '/F', '/PID', str(pid)], check=True)
        else:
            os.kill(pid, signal.SIGTERM)
        click.echo(f"Server (PID: {pid}) has been stopped.")
        logger.info(f"Server (PID: {pid}) has been stopped.")

        # PID 파일 삭제
        pid_file_path = ctx.obj['config']['pid_file']
        if os.path.exists(pid_file_path):
            os.remove(pid_file_path)
            logger.info(f"PID file {pid_file_path} removed.")

    except (subprocess.CalledProcessError, ProcessLookupError) as e:
        click.echo(f"Failed to stop the server. Error: {str(e)}")
        logger.error(f"Failed to stop the server. Error: {str(e)}")


@gms.command()
@click.pass_context
def restart(ctx):
    """Restart the GMS server."""
    ctx.invoke(stop)
    time.sleep(2)
    ctx.invoke(start)


@gms.command()
@click.pass_context
def logs(ctx):
    """Show GMS server logs."""
    if not os.path.exists(ctx.obj['config']['log_file']):
        click.echo(f"Log file {ctx.obj['config']['log_file']} not found.")
        logger.error(f"Log file {ctx.obj['config']['log_file']} not found.")
        return

    def print_logs():
        try:
            with open(ctx.obj['config']['log_file'], 'r') as f:
                # 먼저 마지막 10줄을 출력
                lines = f.readlines()
                for line in lines[-10:]:
                    click.echo(line.strip())

                # 파일의 끝으로 이동
                f.seek(0, 2)
                while not stop_event.is_set():
                    line = f.readline()
                    if line:
                        click.echo(line.strip())
                    else:
                        time.sleep(0.1)
        except Exception as e:
            click.echo(f"Error reading log file: {str(e)}")
            logger.error(f"Error reading log file: {str(e)}")

    click.echo("Showing logs. Press Enter to stop.")
    logger.debug("Starting to show logs")

    stop_event = threading.Event()
    log_thread = threading.Thread(target=print_logs)
    log_thread.daemon = True
    log_thread.start()

    try:
        input()
    finally:
        stop_event.set()
        click.echo("Stopped showing logs.")
        logger.debug("Stopped showing logs")


@gms.command()
@click.pass_context
def status(ctx):
    """Show GMS server status."""
    pid = get_server_pid()
    if not pid:
        click.echo("Server is not running.")
        return

    try:
        import requests
        response = requests.get(f"http://localhost:{ctx.obj['config']['port']}/health")
        if response.status_code == 200:
            click.echo(f"Server is running (PID: {pid})")
            click.echo(response.text)
        else:
            click.echo(f"Server is running (PID: {pid}), but health check failed.")
    except requests.RequestException as e:
        click.echo(f"Server is running (PID: {pid}), but health check failed to connect. Error: {str(e)}")
        logger.error(f"Health check failed. Error: {str(e)}")


@gms.command()
@click.pass_context
def settings(ctx):
    """Show current GMS settings."""
    click.echo("Current settings:")
    for key, value in ctx.obj['config'].items():
        click.echo(f"{key}: {value}")


@gms.command()
@click.pass_context
def reset(ctx):
    """Reset settings and restart GMS server."""
    click.echo("Enter new settings (press Enter to keep current value):")

    for key in ctx.obj['config']:
        if key == 'log_level':
            new_value = click.prompt(
                f"{key}",
                default=ctx.obj['config'][key],
                show_default=True,
                type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False)
            ).upper()
        elif key == 'port':
            new_value = click.prompt(f"{key}", default=ctx.obj['config'][key], show_default=True, type=int)
        else:
            new_value = click.prompt(f"{key}", default=ctx.obj['config'][key], show_default=True)

        if new_value:
            ctx.obj['config'][key] = new_value

    save_config(ctx)
    click.echo("Settings updated. Restarting server...")
    ctx.invoke(restart)


@cli.command()
@click.option('--gms', default='http://localhost:8000', help='GMS server URL')
def ingest(gms):
    """Ingest metadata from qt_meta_populator."""
    try:
        # 서버 상태 확인
        import requests
        try:
            response = requests.get(f"{gms}/health")
            if response.status_code != 200:
                click.echo(f"Server is not healthy. Status code: {response.status_code}")
                return
        except requests.RequestException as e:
            click.echo(f"Failed to connect to server: {str(e)}")
            return

        ingest_metadata.ingest_metadata(gms_server_url=gms)
        click.echo("Metadata ingestion completed successfully.")
    except Exception as e:
        click.echo(f"Error during metadata ingestion: {str(e)}")
        logger.error(f"Error during metadata ingestion: {str(e)}")


@cli.command()
@click.option('--gms', default='http://localhost:8000', help='GMS server URL')
@click.option('--prj_id', required=True, help='Project ID')
def extract(gms, prj_id):
    """Extract lineage from sqlsrc.json."""
    try:
        extract_lineage.extract_lineage(gms_server_url=gms, prj_id=str(prj_id))
        click.echo("Lineage extraction completed successfully.")
    except Exception as e:
        click.echo(f"Error during lineage extraction: {str(e)}")


@cli.command()
@click.option('--gms', default='http://localhost:8000', help='GMS server URL')
@click.option('--prj_id', required=True, help='Project ID')
def move(gms, prj_id):
    """Move from lineage.db to database of qtrack."""
    try:
        move_lineage.move_lineage(gms_server_url=gms, prj_id=str(prj_id))
        click.echo("Lineage movement completed successfully.")
    except Exception as e:
        click.echo(f"Error during lineage movement: {str(e)}")


@cli.command()
@click.option('--gms', default='http://localhost:8000', help='GMS server URL')
@click.option('--prj_id', required=True, help='Project ID')
def batch(gms, prj_id):
    """Run make, extract, and move operations in sequence"""
    try:
        click.echo("Starting batch operation...")

        click.echo("Step 1: making SQL source...")
        make_sqlsrc.make_sqlsrc(prj_id=str(prj_id))

        click.echo("Step 2: Extracting lineage...")
        extract_lineage.extract_lineage(gms_server_url=gms, prj_id=str(prj_id))

        click.echo("Step 3: Moving lineage...")
        move_lineage.move_lineage(gms_server_url=gms, prj_id=str(prj_id))

        click.echo("Batch operation completed successfully.")
    except Exception as e:
        click.echo(f"Error during batch operation: {str(e)}")


if __name__ == "__main__":
    cli(obj={})
