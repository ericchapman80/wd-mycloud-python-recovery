#!/bin/bash
# setup.sh - Automate venv creation and dependency installation
set -e

echo "🚀 Setting up Python virtual environment..."

# Create venv if it doesn't exist
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✅ Created virtual environment in ./venv"
else
    echo "ℹ️  Virtual environment already exists."
fi

# Activate venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install requirements
pip install -r requirements.txt

echo "🎉 Setup complete! To activate your environment later, run:"
echo "source venv/bin/activate"
