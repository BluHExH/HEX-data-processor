#!/bin/bash

# HEX Data Processor Setup Script
# This script sets up the environment for the data processor

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[SETUP]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check Python version
check_python() {
    print_status "Checking Python version..."
    
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
        REQUIRED_VERSION="3.10"
        
        if python3 -c "import sys; exit(0 if sys.version_info >= (3, 10) else 1)"; then
            print_success "Python $PYTHON_VERSION found"
        else
            print_error "Python $REQUIRED_VERSION or higher is required. Found: $PYTHON_VERSION"
            exit 1
        fi
    else
        print_error "Python 3 is not installed"
        exit 1
    fi
}

# Create virtual environment
create_venv() {
    print_status "Creating virtual environment..."
    
    if [ ! -d "venv" ]; then
        python3 -m venv venv
        print_success "Virtual environment created"
    else
        print_warning "Virtual environment already exists"
    fi
}

# Activate virtual environment
activate_venv() {
    print_status "Activating virtual environment..."
    source venv/bin/activate
    print_success "Virtual environment activated"
}

# Upgrade pip
upgrade_pip() {
    print_status "Upgrading pip..."
    pip install --upgrade pip
    print_success "Pip upgraded"
}

# Install dependencies
install_dependencies() {
    print_status "Installing Python dependencies..."
    
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
        print_success "Dependencies installed"
    else
        print_error "requirements.txt not found"
        exit 1
    fi
}

# Create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    
    mkdir -p data/input data/output logs
    print_success "Directories created"
}

# Copy configuration files
setup_config() {
    print_status "Setting up configuration..."
    
    if [ ! -f "config.json" ]; then
        if [ -f "config_example.json" ]; then
            cp config_example.json config.json
            print_success "Configuration copied from example"
        else
            print_error "config_example.json not found"
            exit 1
        fi
    else
        print_warning "config.json already exists"
    fi
}

# Setup pre-commit hooks
setup_precommit() {
    print_status "Setting up pre-commit hooks..."
    
    if command -v pre-commit &> /dev/null; then
        pre-commit install
        print_success "Pre-commit hooks installed"
    else
        print_warning "Pre-commit not available. Install with: pip install pre-commit"
    fi
}

# Setup environment file
setup_env() {
    print_status "Setting up environment file..."
    
    if [ ! -f ".env" ]; then
        if [ -f ".env.example" ]; then
            cp .env.example .env
            print_warning ".env file created from example. Please edit with your settings."
        else
            print_warning ".env.example not found"
        fi
    else
        print_warning ".env file already exists"
    fi
}

# Run tests to verify installation
run_tests() {
    print_status "Running tests to verify installation..."
    
    if command -v pytest &> /dev/null; then
        pytest tests/ -v --tb=short || {
            print_warning "Some tests failed, but installation may still be functional"
        }
        print_success "Tests completed"
    else
        print_warning "pytest not available. Install with: pip install pytest"
    fi
}

# Print setup completion message
print_completion() {
    echo ""
    echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                    SETUP COMPLETED!                        ║${NC}"
    echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${BLUE}Next steps:${NC}"
    echo -e "1. ${YELLOW}Activate the virtual environment:${NC}"
    echo -e "   ${GREEN}source venv/bin/activate${NC}"
    echo ""
    echo -e "2. ${YELLOW}Edit configuration:${NC}"
    echo -e "   ${GREEN}nano config.json${NC}"
    echo ""
    echo -e "3. ${YELLOW}Run the data processor:${NC}"
    echo -e "   ${GREEN}bash scripts/run.sh${NC}"
    echo ""
    echo -e "4. ${YELLOW}Or use the CLI directly:${NC}"
    echo -e "   ${GREEN}python -m src.cli --help${NC}"
    echo ""
    echo -e "5. ${YELLOW}For Docker deployment:${NC}"
    echo -e "   ${GREEN}docker-compose up -d${NC}"
    echo ""
    echo -e "${BLUE}Useful commands:${NC}"
    echo -e "• Validate config: ${GREEN}python -m src.cli validate-config${NC}"
    echo -e "• Run once:       ${GREEN}python -m src.cli run quotes_toscrape --once${NC}"
    echo -e "• Start API:      ${GREEN}python -m src.cli serve${NC}"
    echo -e "• View logs:      ${GREEN}tail -f logs/app.log${NC}"
    echo ""
}

# Main setup function
main() {
    echo -e "${BLUE}"
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║          HEX DATA PROCESSOR - SETUP SCRIPT                  ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    echo ""
    
    check_python
    create_venv
    activate_venv
    upgrade_pip
    install_dependencies
    create_directories
    setup_config
    setup_env
    setup_precommit
    run_tests
    print_completion
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "HEX Data Processor Setup Script"
        echo ""
        echo "Usage: $0 [options]"
        echo ""
        echo "Options:"
        echo "  --help, -h     Show this help message"
        echo "  --skip-tests   Skip running tests"
        echo ""
        exit 0
        ;;
    --skip-tests)
        # Override run_tests function
        run_tests() {
            print_status "Skipping tests as requested"
        }
        main
        ;;
    *)
        main
        ;;
esac