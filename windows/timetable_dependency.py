import datetime
import decimal
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from tkinter import filedialog
import mimetypes
from pathlib import Path

import numpy as np
from sqlalchemy.types import *
import os, sys
sys.path.insert(0, 'windows/')
import timetable_stud
import timetable_fac
import sqlite3
import pandas as pd

def generate_dependency_timetable():
    conn = sqlite3.connect(r'files/timetable.db')
    df = pd.read_sql_query("SELECT * FROM OPERATING_SCHEDULE WHERE os_option = '2'", conn)
    conn.close()
    if df.empty:
        messagebox.showwarning('No Data', 'No data found in OPERATING_SCHEDULE!')
        return
    timetable_dict = df.to_dict('records')
    print(timetable_dict)