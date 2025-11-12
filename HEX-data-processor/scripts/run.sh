#!/bin/bash

# HEX Data Processor Run Script
# This script runs the data processor with various options

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[RUN]${NC} $1"
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

print_info() {
    echo -e "${CYAN}[INFO]${NC} $1"
}

# Check if virtual environment is activated
check_venv() {
    if [[ "$VIRTUAL_ENV" == "" ]]; then
        print_warning "Virtual environment is not activated"
        print_status "Activating virtual environment..."
        
        if [ -d "venv" ]; then
            source venv/bin/activate
            print_success "Virtual environment activated"
        else
            print_error "Virtual environment not found. Please run setup.sh first"
            exit 1
        fi
    else
        print_success "Virtual environment is active: $VIRTUAL_ENV"
    fi
}

# Check configuration
check_config() {
    if [ ! -f "config.json" ]; then
        print_error "Configuration file config.json not found"
        print_status "Creating config from example..."
        
        if [ -f "config_example.json" ]; then
            cp config_example.json config.json
            print_success "Configuration created from example"
            print_warning "Please edit config.json with your settings"
        else
            print_error "config_example.json not found"
            exit 1
        fi
    else
        print_success "Configuration file found"
    fi
}

# Validate configuration
validate_config() {
    print_status "Validating configuration..."
    
    if python -m src.cli validate-config --config config.json; then
        print_success "Configuration is valid"
        return 0
    else
        print_error "Configuration validation failed"
        return 1
    fi
}

# Show menu
show_menu() {
    echo ""
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║              HEX DATA PROCESSOR - RUN MENU                   ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${BLUE}Select an option:${NC}"
    echo -e "  ${GREEN}1${NC}) Run target once (interactive)"
    echo -e "  ${GREEN}2${NC}) Run all targets once"
    echo -e "  ${GREEN}3${NC}) Start scheduler"
    echo -e "  ${GREEN}4${NC}) Start API server"
    echo -e "  ${GREEN}5${NC}) Run with dry-run mode"
    echo -e "  ${GREEN}6${NC}) Export data"
    echo -e "  ${GREEN}7${NC}) Validate configuration"
    echo -e "  ${GREEN}8${NC}) View recent logs"
    echo -e "  ${GREEN}9${NC}) Run tests"
    echo -e "  ${GREEN}10${NC}) Docker deployment"
    echo -e "  ${GREEN}0${NC}) Exit"
    echo ""
}

# Get available targets
get_targets() {
    if [ -f "config.json" ]; then
        python -c "
import json
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
    targets = list(config.get('targets', {}).keys())
    for i, target in enumerate(targets, 1):
        print(f'{i}) {target}')
except Exception as e:
    print('Error reading targets:', e)
"
    else
        print_error "No configuration file found"
        return 1
    fi
}

# Run target once
run_target_once() {
    print_status "Available targets:"
    get_targets
    echo ""
    
    read -p "Select target number: " target_num
    
    # Get target name
    target_name=$(python -c "
import json
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
    targets = list(config.get('targets', {}).keys())
    if 1 <= $target_num <= len(targets):
        print(targets[$target_num - 1])
    else:
        print('')
except:
    print('')
")
    
    if [ -n "$target_name" ]; then
        print_status "Running target: $target_name"
        python -m src.cli --config config.json run "$target_name" --once
    else
        print_error "Invalid target selection"
    fi
}

# Run all targets
run_all_targets() {
    print_status "Running all configured targets..."
    python -m src.cli --config config.json run all --once
}

# Start scheduler
start_scheduler() {
    print_status "Starting scheduler..."
    print_info "Press Ctrl+C to stop"
    python -m src.cli --config config.json run-scheduler
}

# Start API server
start_api() {
    print_status "Starting API server..."
    print_info "API will be available at: http://localhost:8000"
    print_info "Metrics at: http://localhost:8000/metrics"
    print_info "Health check at: http://localhost:8000/health"
    python -m src.cli --config config.json serve --host 0.0.0.0 --port 8000
}

# Run with dry-run
run_dry_run() {
    print_status "Running in dry-run mode..."
    print_status "Available targets:"
    get_targets
    echo ""
    
    read -p "Select target number: " target_num
    
    target_name=$(python -c "
import json
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
    targets = list(config.get('targets', {}).keys())
    if 1 <= $target_num <= len(targets):
        print(targets[$target_num - 1])
    else:
        print('')
except:
    print('')
")
    
    if [ -n "$target_name" ]; then
        print_status "Dry-run for target: $target_name"
        python -m src.cli --config config.json run "$target_name" --once --dry-run
    else
        print_error "Invalid target selection"
    fi
}

# Export data
export_data() {
    echo ""
    echo -e "${BLUE}Export format:${NC}"
    echo "1) CSV"
    echo "2) JSONL"
    echo "3) SQLite"
    echo ""
    
    read -p "Select format: " format_num
    
    case $format_num in
        1) format="csv" ;;
        2) format="jsonl" ;;
        3) format="sqlite" ;;
        *) 
            print_error "Invalid format"
            return 1
            ;;
    esac
    
    read -p "Export path (default: data/output): " export_path
    export_path=${export_path:-"data/output"}
    
    print_status "Exporting data to $format format..."
    python -m src.cli --config config.json export --format "$format" --path "$export_path"
}

# View logs
view_logs() {
    if [ -f "logs/app.log" ]; then
        print_status "Showing recent logs..."
        tail -n 50 logs/app.log
    else
        print_warning "Log file not found: logs/app.log"
    fi
}

# Run tests
run_tests() {
    print_status "Running tests..."
    pytest tests/ -v --tb=short
}

# Docker deployment
docker_deploy() {
    print_status "Deploying with Docker..."
    
    if command -v docker-compose &> /dev/null; then
        docker-compose up -d
        print_success "Docker deployment started"
        print_info "Check status with: docker-compose ps"
        print_info "View logs with: docker-compose logs -f"
    elif command -v docker &> /dev/null; then
        docker compose up -d
        print_success "Docker deployment started"
        print_info "Check status with: docker compose ps"
        print_info "View logs with: docker compose logs -f"
    else
        print_error "Docker or Docker Compose not found"
        print_info "Install Docker to use this option"
    fi
}

# Main function
main() {
    # Display banner
    if [ -f "scripts/banner.txt" ]; then
        cat scripts/banner.txt
        echo ""
    fi
    
    # Check environment
    check_venv
    check_config
    
    # Show menu
    while true; do
        show_menu
        read -p "Enter your choice [0-10]: " choice
        echo ""
        
        case $choice in
            1) run_target_once ;;
            2) run_all_targets ;;
            3) start_scheduler ;;
            4) start_api ;;
            5) run_dry_run ;;
            6) export_data ;;
            7) validate_config ;;
            8) view_logs ;;
            9) run_tests ;;
            10) docker_deploy ;;
            0) 
                print_success "Goodbye!"
                exit 0
                ;;
            *)
                print_error "Invalid choice. Please select 0-10."
                ;;
        esac
        
        echo ""
        read -p "Press Enter to continue..."
        echo ""
    done
}

# Handle command line arguments
if [ $# -gt 0 ]; then
    case "$1" in
        --help|-h)
            echo "HEX Data Processor Run Script"
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --help, -h          Show this help message"
            echo "  --once TARGET       Run target once"
            echo "  --scheduler         Start scheduler"
            echo "  --api               Start API server"
            echo "  --dry-run TARGET    Run target in dry-run mode"
            echo "  --validate          Validate configuration"
            echo ""
            exit 0
            ;;
        --once)
            check_venv
            check_config
            python -m src.cli --config config.json run "$2" --once
            ;;
        --scheduler)
            check_venv
            check_config
            start_scheduler
            ;;
        --api)
            check_venv
            check_config
            start_api
            ;;
        --dry-run)
            check_venv
            check_config
            python -m src.cli --config config.json run "$2" --once --dry-run
            ;;
        --validate)
            check_venv
            check_config
            validate_config
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for available options"
            exit 1
            ;;
    esac
else
    main
fi