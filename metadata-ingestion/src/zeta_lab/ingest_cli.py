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

from zeta_lab.pipeline import ingest_metadata, convert_sqlsrc, extract_lineage, move_lineage
from zeta_lab.utilities.tool import get_server_pid

# 로깅 설정
logging.basicConfig(filename='ingest_cli.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables for configuration
config: Dict[str, Any] = {
    "log_file": "async_lite_gms.log",
    "db_file": "async_lite_gms.db",
    "log_level": "INFO",
    "port": 8000
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
        logging.info(f"Loaded configuration from {config_file}")
    else:
        logging.warning("Configuration file not found. Using default settings.")

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
    logging.info(f"Saved configuration to {config_file}")

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

    if not exec_path:
        click.echo("Error: async_lite_gms executable or script not found. Please ensure it's in the same directory, "
                   "set the ASYNC_LITE_GMS_PATH environment variable, or use --add-data with PyInstaller.")
        return

    cmd = [
        exec_path,
        "--log-file", ctx.obj['config']['log_file'],
        "--db-file", ctx.obj['config']['db_file'],
        "--log-level", ctx.obj['config']['log_level'],
        "--port", str(ctx.obj['config']['port'])
    ]

    try:
        if sys.platform == 'win32':
            if exec_path.endswith('.py'):
                cmd.insert(0, sys.executable)
            process = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NO_WINDOW,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            if exec_path.endswith('.py'):
                cmd.insert(0, sys.executable)
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        time.sleep(2)

        if process.poll() is None:
            click.echo("Server started successfully.")
            logging.info("Server started successfully.")
        else:
            stdout, stderr = process.communicate()
            click.echo(f"Server failed to start. Error: {stderr.decode()}")
            logging.error(f"Server failed to start. Error: {stderr.decode()}")
    except Exception as e:
        click.echo(f"Error starting server: {str(e)}")
        logging.error(f"Error starting server: {str(e)}")


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
        logging.info(f"Server (PID: {pid}) has been stopped.")
    except (subprocess.CalledProcessError, ProcessLookupError) as e:
        click.echo(f"Failed to stop the server. Error: {str(e)}")
        logging.error(f"Failed to stop the server. Error: {str(e)}")


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
        logging.error(f"Log file {ctx.obj['config']['log_file']} not found.")
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
            logging.error(f"Error reading log file: {str(e)}")

    click.echo("Showing logs. Press Enter to stop.")
    logging.debug("Starting to show logs")

    stop_event = threading.Event()
    log_thread = threading.Thread(target=print_logs)
    log_thread.daemon = True
    log_thread.start()

    try:
        input()
    finally:
        stop_event.set()
        click.echo("Stopped showing logs.")
        logging.debug("Stopped showing logs")


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
        logging.error(f"Health check failed. Error: {str(e)}")


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
    """Run ingest_metadata.py"""
    try:
        ingest_metadata.ingest_metadata(gms_server_url=gms)
        click.echo("Metadata ingestion completed successfully.")
    except Exception as e:
        click.echo(f"Error during metadata ingestion: {str(e)}")

@cli.command()
@click.option('--prj_id', required=True, help='Project ID')
def convert(prj_id):
    """Run convert_sqlsrc.py"""
    try:
        convert_sqlsrc.convert_sqlsrc(prj_id=str(prj_id))
        click.echo("SQL source conversion completed successfully.")
    except Exception as e:
        click.echo(f"Error during SQL source conversion: {str(e)}")

@cli.command()
@click.option('--gms', default='http://localhost:8000', help='GMS server URL')
@click.option('--prj_id', required=True, help='Project ID')
def extract(gms, prj_id):
    """Run extract_lineage.py"""
    try:
        extract_lineage.extract_lineage(gms_server_url=gms, prj_id=str(prj_id))
        click.echo("Lineage extraction completed successfully.")
    except Exception as e:
        click.echo(f"Error during lineage extraction: {str(e)}")

@cli.command()
@click.option('--gms', default='http://localhost:8000', help='GMS server URL')
@click.option('--prj_id', required=True, help='Project ID')
def move(gms, prj_id):
    """Run move_lineage.py"""
    try:
        move_lineage.move_lineage(gms_server_url=gms, prj_id=str(prj_id))
        click.echo("Lineage movement completed successfully.")
    except Exception as e:
        click.echo(f"Error during lineage movement: {str(e)}")

@cli.command()
@click.option('--gms', default='http://localhost:8000', help='GMS server URL')
@click.option('--prj_id', required=True, help='Project ID')
def batch(gms, prj_id):
    """Run convert, extract, and move operations in sequence"""
    try:
        click.echo("Starting batch operation...")

        click.echo("Step 1: Converting SQL source...")
        convert_sqlsrc.convert_sqlsrc(prj_id=str(prj_id))

        click.echo("Step 2: Extracting lineage...")
        extract_lineage.extract_lineage(gms_server_url=gms, prj_id=str(prj_id))

        click.echo("Step 3: Moving lineage...")
        move_lineage.move_lineage(gms_server_url=gms, prj_id=str(prj_id))

        click.echo("Batch operation completed successfully.")
    except Exception as e:
        click.echo(f"Error during batch operation: {str(e)}")

if __name__ == "__main__":
    cli(obj={})
