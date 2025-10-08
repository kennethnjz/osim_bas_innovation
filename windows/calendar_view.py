import atexit
import io
import dash_summernote
import sys
import os
import sqlite3
import signal
from flask import request
import threading
import time
import psutil
import setup_start_files

db_path = ""

# Handle imports for both development and executable modes
if getattr(sys, 'frozen', False):
    # Executable mode - add bundled full_calendar_component directory to path
    sys.path.insert(0, os.path.join(sys._MEIPASS, 'full_calendar_component'))
    sys.path.insert(0, sys._MEIPASS)
    sys.path.insert(0, os.path.join(sys._MEIPASS, 'windows'))
else:
    # Development mode
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.insert(0, 'windows/')


import full_calendar_component as fcc
from dash import *
import dash_mantine_components as dmc
from dash.exceptions import PreventUpdate
from datetime import datetime, date, timedelta
import dash_quill
import pandas as pd
import webbrowser
from threading import Timer

app = Dash(__name__, prevent_initial_callbacks='initial_duplicate', external_stylesheets=['https://cdn.jsdelivr.net/npm/summernote@0.8.18/dist/summernote-lite.min.css'], external_scripts=['https://cdn.jsdelivr.net/npm/summernote@0.8.18/dist/summernote-lite.min.js'])

quill_mods = [
    [{"header": "1"}, {"header": "2"}, {"font": []}],
    [{"size": []}],
    ["bold", "italic", "underline", "strike", "blockquote"],
    [{"list": "ordered"}, {"list": "bullet"}, {"indent": "-1"}, {"indent": "+1"}],
    ["link", "image"],
    ["table", ["table"]],
]

# Get today's date                                                                                            
today = datetime.now()

# Format the date
formatted_date = today.strftime("%Y-%m-%d")

df = pd.DataFrame()

def load_data_from_db():
    try:

        print(f"Calendar using database path: {db_path}")
        conn = sqlite3.connect(db_path)
        query = "SELECT series_id, job_id, start_run_datetime, end_run_datetime, dependent_job_id FROM TIMETABLE_DATETIME"
        df_query = pd.read_sql_query(query, conn)
        conn.close()

        df_timetable = pd.DataFrame(columns=['start_date', 'start_time', 'end_date', 'end_time', 'event_name', 'event_color', 'event_context'])

        for idx, row in df_query.iterrows():
            start_date = datetime.strptime(row['start_run_datetime'], "%Y%m%d%H%M").date().strftime("%Y-%m-%d")
            end_date = datetime.strptime(row['end_run_datetime'], "%Y%m%d%H%M").date().strftime("%Y-%m-%d")
            start_time = datetime.strptime(row['start_run_datetime'], "%Y%m%d%H%M").time().strftime("%H:%M:00")
            end_time = datetime.strptime(row['end_run_datetime'], "%Y%m%d%H%M").time().strftime("%H:%M:00")

            new_row = {
                'start_date': start_date,
                'start_time': start_time,
                'end_date': end_date,
                'end_time': end_time,
                'event_name': row["job_id"],
                'event_color': "bg-gradient-secondary",
                'event_context': f'''| Series ID | Job ID | Start Date & Time | End Date & Time | Dependent Job ID |
                | :------: | :------: | :------: | :------: | :------: |
                | {row["series_id"]} | {row["job_id"]} | {start_date} {start_time} | {end_date} {end_time} | {row["dependent_job_id"]} |'''
            }
            df_timetable.loc[len(df_timetable)] = new_row
        return df_timetable
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame()

app.layout = html.Div(
    [
        fcc.FullCalendarComponent(
            id="calendar",  # Unique ID for the component
            initialView="dayGridMonth",  # dayGridMonth, timeGridWeek, timeGridDay, listWeek,
            # dayGridWeek, dayGridYear, multiMonthYear, resourceTimeline, resourceTimeGridDay, resourceTimeLineWeek
            headerToolbar={
                "left": "prev,next today",
                "center": "",
                "right": "listWeek,timeGridDay,timeGridWeek,dayGridMonth",
            },  # Calendar header
            initialDate=f"{formatted_date}",  # Start date for calendar
            editable=False,  # Allow events to be edited
            selectable=True,  # Allow dates to be selected
            events=[],
            nowIndicator=True,  # Show current time indicator
            navLinks=True,  # Allow navigation to other dates
        ),
        dmc.MantineProvider(
            theme={"colorScheme": "dark"},
            children=[
                dmc.Modal(
                    id="modal",
                    size="xl",
                    title="Job Details",
                    zIndex=10000,
                    children=[
                        html.Div(id="modal_event_display_output"),
                        html.Div(id="modal_event_display_context", hidden=True),
                        dmc.Space(h=20),
                        dmc.Group(
                            [
                                dmc.Button(
                                    "Close",
                                    color="red",
                                    variant="outline",
                                    id="modal-close-button",
                                ),
                            ],
                            justify="right",
                        ),
                    ],
                )
            ],
        ),
        dmc.MantineProvider(
            theme={"colorScheme": "dark"},
            children=[
                dmc.Modal(
                    id="add_modal",
                    title="New Event",
                    size="xl",
                    children=[
                        dmc.Grid(
                            children=[
                                dmc.GridCol(
                                    html.Div(
                                        dmc.DatePickerInput(
                                            id="start_date",
                                            label="Start Date",
                                            value=datetime.now().date(),
                                            styles={"width": "100%"},
                                            disabled=True,
                                        ),
                                        style={"width": "100%"},
                                    ),
                                    span=6,
                                ),
                                dmc.GridCol(
                                    html.Div(
                                        dmc.TimePicker(
                                            label="Start Time",
                                            withSeconds=True,
                                            value=datetime.now(),
                                            format="12h",
                                            id="start_time",
                                        ),
                                        style={"width": "100%"},
                                    ),
                                    span=6,
                                ),
                            ],
                            gutter="xl",
                        ),
                        dmc.Grid(
                            children=[
                                dmc.GridCol(
                                    html.Div(
                                        dmc.DatePickerInput(
                                            id="end_date",
                                            label="End Date",
                                            value=datetime.now().date(),
                                            styles={"width": "100%"},
                                        ),
                                        style={"width": "100%"},
                                    ),
                                    span=6,
                                ),
                                dmc.GridCol(
                                    html.Div(
                                        dmc.TimePicker(
                                            label="End Time",
                                            withSeconds=True,
                                            value=datetime.now(),
                                            format="12h",
                                            id="end_time",
                                        ),
                                        style={"width": "100%"},
                                    ),
                                    span=6,
                                ),
                            ],
                            gutter="xl",
                        ),
                        dmc.Grid(
                            children=[
                                dmc.GridCol(
                                    span=6,
                                    children=[
                                        dmc.TextInput(
                                            label="Event Title:",
                                            style={"width": "100%"},
                                            id="event_name_input",
                                            required=True,
                                        )
                                    ],
                                ),
                                dmc.GridCol(
                                    span=6,
                                    children=[
                                        dmc.Select(
                                            label="Select event color",
                                            placeholder="Select one",
                                            id="event_color_select",
                                            value="ng",
                                            data=[
                                                {
                                                    "value": "bg-gradient-primary",
                                                    "label": "bg-gradient-primary",
                                                },
                                                {
                                                    "value": "bg-gradient-secondary",
                                                    "label": "bg-gradient-secondary",
                                                },
                                                {
                                                    "value": "bg-gradient-success",
                                                    "label": "bg-gradient-success",
                                                },
                                                {
                                                    "value": "bg-gradient-info",
                                                    "label": "bg-gradient-info",
                                                },
                                                {
                                                    "value": "bg-gradient-warning",
                                                    "label": "bg-gradient-warning",
                                                },
                                                {
                                                    "value": "bg-gradient-danger",
                                                    "label": "bg-gradient-danger",
                                                },
                                                {
                                                    "value": "bg-gradient-light",
                                                    "label": "bg-gradient-light",
                                                },
                                                {
                                                    "value": "bg-gradient-dark",
                                                    "label": "bg-gradient-dark",
                                                },
                                                {
                                                    "value": "bg-gradient-white",
                                                    "label": "bg-gradient-white",
                                                },
                                            ],
                                            style={"width": "100%", "marginBottom": 10},
                                            required=True,
                                        )
                                    ],
                                ),
                            ]
                        ),
                        dash_summernote.DashSummernote(
                            id='summernote',
                            value='my-value',
                            toolbar=[
                                ["style", ["style"]],
                                ["font", ["bold", "underline", "clear"]],
                                ["fontname", ["fontname"]],
                                ["para", ["ul", "ol", "paragraph"]],
                                ["table", ["table"]],
                                ["insert", ["link", "picture", "video"]],
                                ["view", ["fullscreen", "codeview"]]
                            ],
                            height=300
                        ),
                        dash_summernote.DashSummernote(
                            id='raw_output',
                            value='raw_output',
                            toolbar=[],
                            height=300
                        ),
                        dmc.Accordion(
                            children=[
                                dmc.AccordionItem(
                                    [
                                        dmc.AccordionControl("Raw HTML"),
                                        dmc.AccordionPanel(
                                            html.Div(
                                                id="output_html",
                                                style={
                                                    "height": "300px",
                                                    "overflowY": "scroll",
                                                },
                                            )
                                        ),
                                    ],
                                    value="raw_html",
                                ),
                            ],
                        ),
                        dash_quill.Quill(
                            id="rich_text_input",
                            modules={
                                "toolbar": quill_mods,
                                "clipboard": {
                                    "matchVisual": False,
                                },
                            },
                        ),
                        dmc.Accordion(
                            children=[
                                dmc.AccordionItem(
                                    [
                                        dmc.AccordionControl("Raw HTML"),
                                        dmc.AccordionPanel(
                                            html.Div(
                                                id="rich_text_output",
                                                style={
                                                    "height": "300px",
                                                    "overflowY": "scroll",
                                                },
                                            )
                                        ),
                                    ],
                                    value="raw_html",
                                ),
                            ],
                        ),
                        dmc.Space(h=20),
                        dmc.Group(
                            [
                                dmc.Button(
                                    "Submit",
                                    id="modal_submit_new_event_button",
                                    color="green",
                                ),
                                dmc.Button(
                                    "Close",
                                    color="red",
                                    variant="outline",
                                    id="modal_close_new_event_button",
                                ),
                            ],
                            justify="right",
                        ),
                    ],
                ),
            ],
        ),
    ]
)


@app.callback(
    Output("modal", "opened"),
    Output("modal", "title"),
    Output("modal_event_display_output", "children"),
    Output("modal_event_display_context", "children"),
    Input("modal-close-button", "n_clicks"),
    Input("calendar", "clickedEvent"),
    State("modal", "opened"),
)
def open_event_modal(n, clickedEvent, opened):
    ctx = callback_context

    # print("button Clicked")

    if not ctx.triggered:
        raise PreventUpdate
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "calendar" and clickedEvent is not None:
        event_title = clickedEvent["title"]
        event_context = clickedEvent["extendedProps"]["context"]
        # print("event_context: ")
        # print(event_context)
        return (
            True,
            "Job Details",
            html.Div(
                dcc.Markdown(f'''
                {event_context}
                ''',
                className="md-table")
            ),
            html.Div(
                dash_quill.Quill(
                    id="input3",
                    value=f"{event_context}",
                    modules={
                        "toolbar": False,
                        "clipboard": {
                            "matchVisual": False,
                        },
                    },
                ),
                style={"width": "100%", "overflowY": "auto"},
            ),
        )
    elif button_id == "modal-close-button" and n is not None:
        return False, no_update, no_update, no_update

    return opened, no_update


@app.callback(
    Output("add_modal", "opened"),
    Output("start_date", "value"),
    Output("end_date", "value"),
    Output("start_time", "value"),
    Output("end_time", "value"),
    Input("calendar", "dateClicked"),
    Input("modal_close_new_event_button", "n_clicks"),
    State("add_modal", "opened"),
)
def open_add_modal(dateClicked, close_clicks, opened):

    ctx = callback_context

    if not ctx.triggered:
        raise PreventUpdate
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    raise PreventUpdate

    # if button_id == "calendar" and dateClicked is not None:
    #     try:
    #         start_time = datetime.strptime(dateClicked, "%Y-%m-%dT%H:%M:%S%z").time()
    #         start_date_obj = datetime.strptime(dateClicked, "%Y-%m-%dT%H:%M:%S%z")
    #         start_date = start_date_obj.strftime("%Y-%m-%d")
    #         end_date = start_date_obj.strftime("%Y-%m-%d")
    #     except ValueError:
    #         start_time = datetime.now().time()
    #         start_date_obj = datetime.strptime(dateClicked, "%Y-%m-%d")
    #         start_date = start_date_obj.strftime("%Y-%m-%d")
    #         end_date = start_date_obj.strftime("%Y-%m-%d")
    #     end_time = datetime.combine(date.today(), start_time) + timedelta(hours=1)
    #     start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    #     end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    #     return True, start_date, end_date, start_time_str, end_time_str
    #
    # elif button_id == "modal_close_new_event_button" and close_clicks is not None:
    #     return False, no_update, no_update, no_update, no_update

    return opened, no_update, no_update, no_update, no_update

# Test Dataframe
# df = pd.DataFrame({
#    'start_date' : ["2025-09-08"],
#    'start_time' : ["19:56:00"],
#    'end_date' : ["2025-09-02"],
#    'end_time' : ["21:56:00"],
#    'event_name' : ["LISD056"],
#    'event_color' : ["bg-gradient-primary"],
#    'event_context' : ["Job Description"]
# })

@app.callback(
    Output("calendar", "events"),
    Output("add_modal", "opened", allow_duplicate=True),
    Output("event_name_input", "value"),
    Output("event_color_select", "value"),
    Output("rich_text_input", "value"),
    Input("modal_submit_new_event_button", "n_clicks"),
    State("start_date", "value"),
    State("start_time", "value"),
    State("end_date", "value"),
    State("end_time", "value"),
    State("event_name_input", "value"),
    State("event_color_select", "value"),
    State("rich_text_output", "children"),
    State("calendar", "events"),
)
def add_new_event(
    n,
    start_date,
    start_time,
    end_date,
    end_time,
    event_name,
    event_color,
    event_context,
    current_events,
):
    # print(type(current_events))
    print(n)
    new_events = list()
    if n is None:
        # print("Avoiding update")
        # print(df.to_json())
        for idx, row in df.iterrows():
            start_date = row['start_date']
            start_time = row['start_time']
            end_date = row['end_date']
            end_time = row['end_time']
            event_name = row['event_name']
            event_color = row['event_color']
            event_context = row['event_context']

        # raise PreventUpdate
            # print(start_date, start_time, end_date, end_time, event_name, event_color)
            # print()
            # print('time_obj')
            start_time_obj = datetime.strptime(start_date + " " +  start_time, "%Y-%m-%d %H:%M:%S")
            # print(start_time_obj)
            if 'T' in end_time:
                end_time_obj = datetime.strptime(end_date + "T" + end_time, "%Y-%m-%dT%H:%M:%S")
            else:
                end_time_obj = datetime.strptime(end_date + " " + end_time, "%Y-%m-%d %H:%M:%S")
            # print(end_time_obj)

            # print('time_str')
            start_time_str = start_time_obj.strftime("%H:%M:%S")
            # print(start_time_str)
            end_time_str = end_time_obj.strftime("%H:%M:%S")
            # print(end_time_str)

            start_date = f"{start_date}T{start_time_str}"
            end_date = f"{end_date}T{end_time_str}"

            new_event = {
                "title": event_name,
                "start": start_date,
                "end": end_date,
                "className": event_color,
                "context": event_context,
            }

            new_events.append({
                "title": event_name,
                "start": start_date,
                "end": end_date,
                "className": event_color,
                "extendedProps": {"context": event_context}
            })

            #new_events = new_events.append(new_event)

            # print("new_event:")
            # print(new_event)
            #
            # print("new_events")
            # print(new_events)
            #
            # print("current events: ")
            # print((current_events + new_events))
    else:
        start_time_obj = datetime.strptime(start_date + " " +  start_time, "%Y-%m-%d %H:%M:%S")
        # print(start_time_obj)
        if 'T' in end_time:
            end_time_obj = datetime.strptime(end_date + "T" + end_time, "%Y-%m-%dT%H:%M:%S")
        else:
            end_time_obj = datetime.strptime(end_date + " " + end_time, "%Y-%m-%d %H:%M:%S")
        # print(end_time_obj)

        # print('time_str')
        start_time_str = start_time_obj.strftime("%H:%M:%S")
        # print(start_time_str)
        end_time_str = end_time_obj.strftime("%H:%M:%S")
        # print(end_time_str)

        start_date = f"{start_date}T{start_time_str}"
        end_date = f"{end_date}T{end_time_str}"

        new_event = {
            "title": event_name,
            "start": start_date,
            "end": end_date,
            "className": event_color,
            "context": event_context,
        }

        new_events.append({
            "title": event_name,
            "start": start_date,
            "end": end_date,
            "className": event_color,
            "context": event_context,
        })

        #new_events = new_events.append(new_event)

        # print("new_event:")
        # print(new_event)
        #
        # print("new_events")
        # print(new_events)
        #
        # print("current events: ")

    # print((current_events + new_events))

    return current_events + new_events, False, "", "bg-gradient-secondary", ""


@app.callback(
    Output("rich_text_output", "children"),
    [Input("rich_text_input", "value")],
    [State("rich_text_input", "charCount")],
)
def display_output(value, charCount):
    return value

@app.callback(Output('output_html', 'children'),
              Output('raw_output', 'value'),
              Input('summernote', 'value'))
def display_output_html(value):
    # print("value: ")
    # print(value)
    return value, value

last_request_time = time.time()

def monitor_requests():
    """Monitor HTTP requests to detect tab activity"""
    global last_request_time
    while True:
        time.sleep(10)
        if time.time() - last_request_time > 300:  # 300 seconds timeout
            print("No requests from tab, shutting down...")
            cleanup()
            break

@app.server.before_request
def track_requests():
    """Track incoming requests"""
    global last_request_time
    last_request_time = time.time()

 # Function to open the browser
def open_browser():
    # Check if the server is already running in a different process
    if not os.environ.get("WERKZEUG_RUN_MAIN"):
        # print("Open Browser")
        # Open the default browser to the app's URL
        webbrowser.open_new("http://127.0.0.1:8056/")
        # Start request monitoring thread
        monitor_thread = threading.Thread(target=monitor_requests, daemon=True)
        monitor_thread.start()

@app.server.route('/shutdown', methods=['POST'])
def shutdown():
    # os.kill(os.getpid(), signal.SIGINT)

    # This will only work with the development server
    # For production deployments, you would need a different shutdown mechanism
    # (e.g., using a process manager like Gunicorn with a graceful shutdown signal)
    # func = request.environ.get('werkzeug.server.shutdown')
    # if func is None:
    #     raise RuntimeError('Not running with the Werkzeug Server')
    # func()
    # return 'Server shutting down...'

    """Shutdown endpoint"""
    print("Shutdown request received")
    cleanup()
    return 'Shutting down...'

def cleanup():
    """Cleanup function to ensure proper shutdown"""
    print("Cleaning up calendar process...")
    os._exit(0)

def signal_handler(signum, frame):
    """Handle termination signals"""
    print(f"Received signal {signum}, shutting down...")
    cleanup()

if __name__ == "__main__":
    # Register cleanup handlers
    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = setup_start_files.get_database_path()
        print("No arguments received.")

    try:
        # Load initial data from database
        df = load_data_from_db()

        # Schedule the browser to open 1 second after the server starts
        Timer(1, open_browser).start()
        app.run(debug=False, port=8056, host='127.0.0.1', use_reloader=False)
    except KeyboardInterrupt:
        print("KeyboardInterrupt received, shutting down...")
        cleanup()
    except Exception as e:
        print(f"Error in calendar app: {e}")
        cleanup()
    # # Load initial data from database
    # df = load_data_from_db()
    # # Schedule the browser to open 1 second after the server starts
    # Timer(1, open_browser).start()
    # app.run(debug=False, port=8056)

    # if len(sys.argv) > 1:
    #     # print(f"Arguments received: {sys.argv[1]}")
    #     df_new = pd.read_json(io.StringIO(sys.argv[1]), convert_dates=False)
    #     df = pd.concat([df,df_new], ignore_index=True)
    #     print(df.to_json())
    # else:
    #     print("No arguments received.")
    # app.run(debug=True, port=8056)
    # # Schedule the browser to open 1 second after the server starts
    # Timer(1, open_browser).start()