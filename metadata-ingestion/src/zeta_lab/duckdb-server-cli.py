import signal
import subprocess
import sys
from typing import Optional

import click
import requests
import yaml

SERVER_PROCESS = None

def load_config(config_file: str) -> dict:
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def save_config(config: dict, config_file: str):
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

@click.group()
def cli():
    """CLI for managing the AsyncDuckDBServer"""
    pass

@cli.command()
@click.option('--config', default='config.yml', help='Path to the configuration file')
@click.option('--port', default=None, type=int, help='Port to run the server on')
@click.option('--log-file', default=None, help='Path to the log file')
def start(config: str, port: Optional[int], log_file: Optional[str]):
    """Start the AsyncDuckDBServer"""
    global SERVER_PROCESS
    if SERVER_PROCESS:
        click.echo("Server is already running.")
        return

    config_data = load_config(config)
    if port is not None:
        config_data['server']['port'] = port
    if log_file is not None:
        config_data['logging']['file'] = log_file
    save_config(config_data, config)

    cmd = [
        sys.executable,
        'async_duckdb_server.py',
        '--config', config,
    ]

    log_file = config_data['logging']['file']
    with open(log_file, 'w') as log:
        SERVER_PROCESS = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    
    click.echo(f"Server started on port {config_data['server']['port']}. Logs are being written to {log_file}")

@cli.command()
def stop():
    """Stop the AsyncDuckDBServer"""
    global SERVER_PROCESS
    if SERVER_PROCESS:
        SERVER_PROCESS.send_signal(signal.SIGTERM)
        SERVER_PROCESS.wait()
        SERVER_PROCESS = None
        click.echo("Server stopped.")
    else:
        click.echo("Server is not running.")

@cli.command()
@click.option('--config', default='config.yml', help='Path to the configuration file')
def health(config: str):
    """Check the health of the AsyncDuckDBServer"""
    config_data = load_config(config)
    host = f"http://localhost:{config_data['server']['port']}"
    try:
        response = requests.get(f"{host}/health")
        click.echo(response.json())
    except requests.RequestException as e:
        click.echo(f"Error connecting to server: {e}")

@cli.command()
@click.option('--config', default='config.yml', help='Path to the configuration file')
def metrics(config: str):
    """Get metrics from the AsyncDuckDBServer"""
    config_data = load_config(config)
    host = f"http://localhost:{config_data['server']['port']}"
    try:
        response = requests.get(f"{host}/metrics")
        click.echo(yaml.dump(response.json(), default_flow_style=False))
    except requests.RequestException as e:
        click.echo(f"Error connecting to server: {e}")

@cli.command()
@click.argument('key')
@click.argument('value')
@click.option('--config', default='config.yml', help='Path to the configuration file')
def set_config(key: str, value: str, config: str):
    """Set a configuration value"""
    config_data = load_config(config)
    keys = key.split('.')
    current = config_data
    for k in keys[:-1]:
        if k not in current:
            current[k] = {}
        current = current[k]
    current[keys[-1]] = value
    save_config(config_data, config)
    click.echo(f"Set {key} to {value} in the configuration.")

@cli.command()
@click.option('--config', default='config.yml', help='Path to the configuration file')
def show_config(config: str):
    """Show the current configuration"""
    config_data = load_config(config)
    click.echo(yaml.dump(config_data, default_flow_style=False))

if __name__ == '__main__':
    cli()
