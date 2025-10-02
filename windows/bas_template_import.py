import datetime
import decimal
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from tkinter import filedialog
import mimetypes
from pathlib import Path

import numpy as np
import os, sys
sys.path.insert(0, 'windows/')
import pandas as pd
from datetime import datetime, timedelta
import setup_start_files
import re
import spacy
from spacy.matcher import Matcher
import numpy as np

def add_space_after_punct(text):
    # Add space after comma or period if not followed by space already
    # Look for ',' or '.' followed immediately by a non-space character
    return re.sub(r'([,.])(?=\S)', r'\1 ', text)

def has_negation(token):
    """
    Checks if the token or its auxiliaries are negated (has a 'neg' child).
    """
    for child in token.children:
        if child.dep_ == "neg":
            return True
    return False

def is_negated_run_after(doc):
    """
    Checks if there's a negation around the phrase 'run after' or similar.
    Returns True if negated, False otherwise.
    """
    for token in doc:
        # Look for verb "run" or auxiliaries
        if token.lemma_ == "run" and token.pos_ == "VERB":
            # Check if token or auxiliaries have negation
            if has_negation(token):
                return True
            # Also check auxiliaries (e.g. do, does, don't)
            for aux in token.children:
                if aux.dep_ == "aux" and has_negation(aux):
                    return True

            # Extra: also check token's head if it's a verb (for complex sentences)
            if token.head.pos_ == "VERB" and has_negation(token.head):
                return True

    return False

def split_on_conjunctions(doc):
    """
    Split doc into clauses at coordinating conjunctions ('and', 'but').
    Returns list of spacy spans with no leading/trailing spaces.
    """
    clauses = []
    start = 0
    for token in doc:
        if token.dep_ == "cc":
            clause = doc[start:token.i].text.strip()
            clauses.append(nlp(clause))  # re-parse to get clean span
            start = token.i + 1
    clause = doc[start:len(doc)].text.strip()
    clauses.append(nlp(clause))
    return clauses

nlp = spacy.load("en_core_web_sm")
matcher = Matcher(nlp.vocab)

# Pattern to capture job codes like LISD001 or RSSD058@
job_pattern = [{"TEXT": {"REGEX": "^[A-Z]{4}\\d{3}@?$"}}]
matcher.add("JOB_CODE", [job_pattern])

# Pattern to capture delays like '15 minutes after'
delay_patterns = [
    [
        {"LIKE_NUM": True},
        {"LOWER": {"IN": ["minute", "minutes", "min", "mins"]}},
        {"LOWER": "after"}
    ],
    [
        {"LOWER": "after"},
        {"LIKE_NUM": True},
        {"LOWER": {"IN": ["minute", "minutes", "min", "mins"]}}
    ]
]
matcher.add("DELAY", delay_patterns)

# Pattern to capture start time like 'at 11PM'
time_pattern = [
    {"LOWER": "at"},
    {"TEXT": {"REGEX": r"^\d{1,2}(:\d{2})?\s?(AM|PM|am|pm)$"}}
]
matcher.add("START_TIME", [time_pattern])

def normalize_time(time_str):
    try:
        return datetime.strptime(time_str.upper(), "%I%p").strftime("%H%M")
    except ValueError:
        return time_str
    
def parse_with_spacy(text):
    text = add_space_after_punct(text)
    doc = nlp(text)

    dependency_jobs = []
    dependency_delay = ""
    start_run_time = ""
    exclude_holiday = "1" if "exclude ph" in text.lower() or "excluding ph" in text.lower() else "0"

    # Split sentence by conjunctions
    clauses = split_on_conjunctions(doc)
    for clause in clauses:
      negated = is_negated_run_after(clause)
      matches = matcher(clause)
      for match_id, start, end in matches:
          span = clause[start:end]
          label = nlp.vocab.strings[match_id]

          if label == "JOB_CODE":
              if not negated:
                for token in span:
                    if token.text not in dependency_jobs:
                        dependency_jobs.append(token.text)
          elif label == "DELAY":
              for token in span:
                  if token.like_num:
                      dependency_delay = token.text
                      break
          elif label == "START_TIME":
              # span is like "at 11PM", so take second token
              start_run_time = normalize_time(span[1].text.upper())

    return {
        "dependency_jobs": dependency_jobs,
        "dependency_delay": dependency_delay,
        "start_run_time": start_run_time,
        "exclude_holiday": exclude_holiday
    }

def convert_range_lower(value):
    if pd.isna(value):
        return np.nan
    value = str(value).strip().lower()

    if '-' in value:
        try:
            # Handle ranges like '1k-5k' by taking the upper bound
            return float(value.split('-')[1].strip().replace('k', 'e3').replace('m', 'e6'))
        except ValueError:
            return np.nan
    elif value.endswith('m'):
        try:
            return float(value[:-1]) * 1_000_000
        except ValueError:
            return np.nan
    elif value.endswith('k'):
        try:
            return float(value[:-1]) * 1_000
        except ValueError:
            return np.nan
    else:
        try:
            return float(value)
        except ValueError:
            return np.nan
        
def apply_spacy_parsing(df, source_col="Remarks"):
    df = df.copy()

    # Apply your spaCy-based parser to the target column
    parsed_data = df[source_col].fillna("").apply(parse_with_spacy)

    # parsed_data is a Series of dicts; convert to a DataFrame and concat
    parsed_df = pd.DataFrame(parsed_data.tolist())

    # Join extracted columns back to the main DataFrame
    return pd.concat([df, parsed_df], axis=1)

def assign_series_ids_by_title(df):
    df = df.copy()

    # Create mapping from unique titles to S001, S002, ...
    unique_titles = df["Title of Run Series"].unique()
    title_to_series_id = {
        title: f"S{i:03}" for i, title in enumerate(unique_titles, start=1)
    }

    # Map Series ID using the title
    df["Series ID"] = df["Title of Run Series"].map(title_to_series_id)

    return df

def import_bas_template(file_type, filename):
    try:
        if file_type == ".xlsx":
            ds = pd.read_excel(filename, sheet_name="OM")
    except FileNotFoundError:
        print(f"Error: File not found at '{filename}'")
    except ValueError as ve:
        print(f"Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    if ds.empty:
        messagebox.showwarning('No File', 'No file found!')
        return

    # List of columns you want to select
    columns_to_select = [
        'Function of SRS',
        'Title of Run Series',
        'Job Name',
        'Job Run Mode',
        'Estimated Run Time\n(minutes)',  # note: keep the \n if it exists exactly, else remove it
        'Estimated Volume of records to be processed',
        'Scheduling Instructions',
        'Description of the batch job',
        'Problem Ticket Management Priority Level',
        'Server Name',
        'Script   ',  # note: extra spaces might be present, confirm with ds.columns
        'First Run Date',
        'Last Run Date',
        'Remarks'
    ]

    # You can clean trailing spaces in column names if needed
    ds.columns = ds.columns.str.strip()

    # Adjust columns_to_select for stripped names
    columns_to_select = [col.strip() for col in columns_to_select]

    # Filter existing columns just to be safe
    existing_cols = [col for col in columns_to_select if col in ds.columns]

    # Create new dataframe with selected columns
    newDs = ds[existing_cols].copy()

    newDs["First Run Date"] = pd.to_datetime(
        newDs["First Run Date"], errors='coerce'
    ).dt.strftime('%Y%m%d')

    newDs["Last Run Date"] = pd.to_datetime(
        newDs["Last Run Date"], errors='coerce'
    ).dt.strftime('%Y%m%d')

    newDs['Estimated Volume of records to be processed'] = newDs['Estimated Volume of records to be processed'].apply(convert_range_lower)
    newDs = apply_spacy_parsing(newDs, source_col="Scheduling Instructions")
    newDs = assign_series_ids_by_title(newDs)

    mapping = {
    "Function of SRS": "SRS Function String (200)",
    "Series ID": "Series ID String (10)",
    "Title of Run Series": "Series Title String (200)",
    "Job Name": "Job ID String (8)",
    "Description of the batch job" : "Job Description String (200)",
    "Remarks" : "Remarks/Other Operating Instructions String (500)",
    "First Run Date": "Starting Run Date (YYYYMMDD) Date/Integer (8)",
    "Last Run Date": "Ending Run Date (YYYYMMDD) Date/Integer (8)",
    "Job Run Mode": "Run Mode String (10)",
    "Estimated Volume of records to be processed": "Estimated Transaction Volume Integer (8)",
    "Estimated Run Time\n(minutes)": "Estimated Run Time (In Number of Minutes) Integer (4)",
    "Problem Ticket Management Priority Level": "Problem Ticket Management Priority Level Integer (1) Options: Critical (1), High (2), Medium (3 & 4), Low (5)",
    "Server Name": "Server Name String (500)",
    "Script": "Script String (500)",
    "By Schedule Or By Dependency": "By Schedule/By Dependency Option Integer (1) Options: By Schedule (1), By Dependency (2)",
    "Scheduling Type": "Schedule Type Integer (1) Options: Daily (1), Weekly (2)",
    "Month": "Month String (Integer, semi-colon delimited) Options: January (1) - December (12)",
    "Week Number": "Week Number Integer (1) Options: 1 - 4",
    "Day Number": "Day Number Integer (2) Options: 1 - 20 for Monthly(Day of Month) & Quarterly (Day of Quarter) & , Yearly (Day of Month), 1 - 7 for Monthly (Final X Day of Month) & Yearly (Final X Day of ",
    "Yearly Run Date": "Yearly Run Date (YYYYMMDD) Date/Integer (8)",
    "Days of Week": "Days of Week String (Integer, semi-colon delimited) Options: Monday (1) - Sunday (7)",
    "exclude_holiday": "Exclude Public Holidays Boolean",
    "start_run_time": "Start Time /Integer (4) Options: 0000 - 2359",
    "dependency_jobs": "Dependent Job ID String (8)",
    "dependency_delay": "Minutes after Successful Dependent Job ID Integer (2) Options: 0 - 30"
    }

    # Assuming ds is your DataFrame
    # Rename columns that are in the mapping keys
    newDs = newDs.rename(columns={k: v for k, v in mapping.items() if k in newDs.columns})
    # Define desired order from mapping values
    desired_col_order = list(mapping.values())

    # Keep only columns present in newDs
    cols_in_df = [col for col in desired_col_order if col in newDs.columns]

    # Reorder columns
    newDs = newDs[cols_in_df]

    newDs["Dependent Job ID String (8)"] = newDs["Dependent Job ID String (8)"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else x
    )

    messagebox.showerror("Generated OSIM Template", "BAS templat has been converted to OSIM template!")
    save = messagebox.askyesno("Save Template", "Do you want to save the Generated OSIM Template?")

    if save:
        report_file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")])

        if report_file_path:
            try:
                newDs.to_excel(report_file_path, index=False)
                messagebox.showinfo("Saved", f"Template saved to:\n{report_file_path}")
                return
            except Exception as e:
                messagebox.showerror("Save Failed", f"Could not save file:\n{str(e)}")