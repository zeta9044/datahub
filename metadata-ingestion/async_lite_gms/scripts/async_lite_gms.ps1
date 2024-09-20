# Set the execution policy to allow running scripts (you might need to run this as an administrator)
# Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

# Move to the project root directory
$projectRoot = "C:\git\datahub\metadata-ingestion"
Set-Location -Path $projectRoot

# Clean up previous build artifacts while preserving important files
if (Test-Path "build\async_lite_gms") {
    Get-ChildItem -Path "build\async_lite_gms" -Exclude @("spec", "*.ps1") | Remove-Item -Recurse -Force
    if (Test-Path "build\async_lite_gms\spec") {
        Get-ChildItem -Path "build\async_lite_gms\spec" -Exclude @("async_lite_gms.spec", "*.ps1", "*.ico") | Remove-Item -Recurse -Force
    }
}
if (Test-Path "dist") {
    Remove-Item -Path "dist" -Recurse -Force
}

# Activate virtual environment and execute commands
& "$PWD\venv\Scripts\Activate.ps1"

if ($?) {
    # Generate binary using PyInstaller
    pyinstaller --clean build\async_lite_gms\spec\async_lite_gms.spec

    if ($?) {
        $outputPath = Join-Path -Path $projectRoot -ChildPath "dist\async_lite_gms.exe"
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