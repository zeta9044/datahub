import os
import signal
import subprocess
import threading
import psutil
import json
import time
import sys
import logging
import click
from typing import Dict, Any

# 로깅 설정
logging.basicConfig(filename='ingest_cli.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables for configuration
config: Dict[str, Any] = {
    "log_file": "async_lite_gms.log",
    "db_file": "meta.db",
    "log_level": "INFO",
    "port": 8000
}

def load_config():
    global config
    if os.path.exists('config.json'):
        with open('config.json', 'r') as f:
            loaded_config = json.load(f)
            # 로그 레벨을 대문자로 변환
            if 'log_level' in loaded_config:
                loaded_config['log_level'] = loaded_config['log_level'].upper()
            config.update(loaded_config)


def save_config():
    with open('config.json', 'w') as f:
        json.dump(config, f, indent=2)

def get_server_pid():
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        if 'python' in proc.info['name'].lower() and 'async_lite_gms.py' in ' '.join(proc.info['cmdline']):
            return proc.info['pid']
    return None

@click.group()
@click.pass_context
def cli(ctx):
    """Ingestion CLI for managing the server and other operations."""
    ctx.ensure_object(dict)
    load_config()
    ctx.obj['config'] = config

@cli.group()
@click.pass_context
def meta(ctx):
    """Commands for managing the server metadata."""
    pass

@meta.command()
@click.pass_context
def start(ctx):
    """Start the server."""
    pid = get_server_pid()
    if pid:
        click.echo(f"Server is already running (PID: {pid})")
        return

    cmd = [
        sys.executable, "async_lite_gms.py",
        "--log-file", ctx.obj['config']['log_file'],
        "--db-file", ctx.obj['config']['db_file'],
        "--log-level", ctx.obj['config']['log_level'],
        "--port", str(ctx.obj['config']['port'])
    ]

    try:
        if sys.platform == 'win32':
            process = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NO_WINDOW,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
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

@meta.command()
@click.pass_context
def stop(ctx):
    """Stop the server."""
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

@meta.command()
@click.pass_context
def restart(ctx):
    """Restart the server."""
    ctx.invoke(stop)
    time.sleep(2)
    ctx.invoke(start)

@meta.command()
@click.pass_context
def logs(ctx):
    """Show server logs."""
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

@meta.command()
@click.pass_context
def status(ctx):
    """Show server status."""
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

@meta.command()
@click.pass_context
def settings(ctx):
    """Show current settings."""
    click.echo("Current settings:")
    for key, value in ctx.obj['config'].items():
        click.echo(f"{key}: {value}")

@meta.command()
@click.pass_context
def reset(ctx):
    """Reset settings and restart server."""
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

    save_config()
    click.echo("Settings updated. Restarting server...")
    ctx.invoke(restart)

if __name__ == "__main__":
    cli(obj={})