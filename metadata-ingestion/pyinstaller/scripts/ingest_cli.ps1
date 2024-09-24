# Set the execution policy to allow running scripts (you might need to run this as an administrator)
# Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

# Move to the project root directory
$projectRoot = "C:\git\datahub\metadata-ingestion"
Set-Location -Path $projectRoot

# Function to remove items with error handling
function Remove-ItemSafely {
    param(
        [string]$Path
    )
    if (Test-Path $Path) {
        try {
            Remove-Item -Path $Path -Recurse -Force -ErrorAction Stop
        }
        catch {
            Write-Host "Warning: Could not remove $Path. Continuing anyway." -ForegroundColor Yellow
        }
    }
}

# Clean up previous build artifacts while preserving important files
Remove-ItemSafely -Path "build\ingest_cli"
#Remove-ItemSafely -Path "dist"

# Activate virtual environment and execute commands
& "$PWD\venv\Scripts\Activate.ps1"

if ($?) {
    # Generate binary using PyInstaller
    pyinstaller --clean pyinstaller\spec\ingest_cli.spec

    if ($?) {
        $outputPath = Join-Path -Path $projectRoot -ChildPath "dist\ingest_cli.exe"
        Write-Host "Build completed successfully." -ForegroundColor Green
        Write-Host "The final executable is located at:" -ForegroundColor Green
        Write-Host $outputPath -ForegroundColor Cyan
    } else {
        Write-Host "An error occurred during the PyInstaller build process." -ForegroundColor Red
    }

    # Deactivate virtual environment
    deactivate
} else {
    Write-Host "Failed to activate the virtual environment." -ForegroundColor Red
}

# Check for overall script success
if ($LASTEXITCODE -ne 0) {
    Write-Host "An error occurred during the build process." -ForegroundColor Red
    exit 1
}