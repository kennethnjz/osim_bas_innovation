# Job Scheduling & Timetable Management System

## Overview
A comprehensive Python-based system for managing batch job execution schedules with dependency resolution, calendar visualization, and public holiday management.

## Core Features
- **Schedule Import/Export**: Excel/CSV template processing with validation
- **Dependency Management**: Complex job dependency chains with automatic resolution
- **Timetable Generation**: 14-day rolling schedules with multiple frequency types
- **Calendar Visualization**: Interactive web-based calendar and Gantt charts
- **Public Holiday Integration**: Automatic schedule exclusions for holidays

## Architecture

### Main Application (`main.py`)
- Tkinter-based GUI interface
- Schedule import/export functionality
- Timetable generation orchestration
- Calendar view integration

### Database Layer (`db_setup.py`)
- SQLite database with 17 normalized tables
- Core tables: `OPERATING_SCHEDULE`, `TIMETABLE_DATETIME`, `JOB`, `RUN_SERIES`
- Dependency tracking: `JOB_DEPENDENCY`
- Holiday management: `PUBLIC_HOLIDAY`

### Processing Modules

#### Schedule Validation (`schedule_template_validation.py`)
- 25-column template validation
- Date/time format checking (YYYYMMDD/HHMM)
- Business rule enforcement
- Error reporting with Excel export

#### Timetable Engine (`timetable_generation.py`)
- Dependency resolution algorithm
- Schedule expansion for 14-day periods
- Time calculation with estimated run durations
- Support for Daily/Weekly/Monthly frequencies

#### Database Population (`populate_db_from_schedule.py`)
- Transforms flat schedules into normalized database structure
- Handles job series, dependencies, and timetable records
- Complex scheduling pattern support

### Visualization Components

#### Calendar Interface (`calendar_view.py`)
- Dash-based web application
- FullCalendar component integration
- Interactive job details with markdown support
- Real-time schedule viewing

#### Gantt Charts (`ganttchart.py`)
- Plotly-based timeline visualization
- Job dependency relationships
- Schedule overview capabilities

### Specialized Features

#### Public Holiday Management (`public_holiday.py`)
- Holiday calendar import/export
- Automatic schedule conflict resolution
- Year-based holiday management

#### Schedule Exclusions (`timetable_exclude_ph.py`)
- Public holiday exclusion processing
- Schedule integrity maintenance

## Technical Stack
- **Backend**: Python 3.x, SQLite, Pandas
- **GUI**: Tkinter
- **Web Interface**: Dash, Plotly, FullCalendar
- **Data Processing**: Pandas, datetime manipulation
- **File Formats**: Excel (.xlsx), CSV support

## Key Capabilities

### Schedule Management
- Import templates with comprehensive validation
- Support for complex job dependencies
- Multiple scheduling frequencies (D/W/M)
- Automated 14-day rolling timetable generation

### Job Dependencies
- Hierarchical job execution chains
- Dependency resolution with circular detection
- Configurable delay times between dependent jobs
- Root job identification and processing

### Data Validation
- Template structure validation
- Date/time format verification
- Permissible value checking
- Comprehensive error reporting

### Visualization
- Interactive web-based calendar
- Gantt chart timeline views
- Job detail popups with dependency information
- Real-time schedule updates

## Usage Workflow
1. **Import**: Load schedule templates (Excel/CSV) with validation
2. **Process**: System populates normalized database tables
3. **Generate**: Create 14-day rolling timetables with dependencies
4. **Visualize**: View schedules in calendar or Gantt chart format
5. **Export**: Save processed schedules in various formats

## File Structure
```
├── main.py                          # Main application entry point
├── windows/                         # Core processing modules
│   ├── db_setup.py                 # Database schema and setup
│   ├── timetable_generation.py     # Timetable generation engine
│   ├── schedule_template_validation.py # Template validation
│   ├── populate_db_from_schedule.py # Database population
│   ├── calendar_view.py            # Web calendar interface
│   ├── ganttchart.py              # Gantt chart visualization
│   ├── public_holiday.py          # Holiday management
│   └── timetable_exclude_ph.py    # Holiday exclusion processing
├── files/
│   └── timetable.db               # SQLite database
└── requirements.txt               # Python dependencies
```
