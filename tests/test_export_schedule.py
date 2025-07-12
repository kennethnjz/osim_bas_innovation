import unittest
from unittest.mock import patch, MagicMock
import main

class TestExportSchedule(unittest.TestCase):
    @patch('main.filedialog.asksaveasfilename')
    @patch('main.messagebox.showinfo')
    @patch('main.messagebox.showwarning')
    @patch('main.messagebox.showerror')
    @patch('main.pd.DataFrame.to_excel')
    @patch('main.pd.DataFrame.to_csv')
    @patch('main.pd.read_sql_query')
    @patch('main.sqlite3.connect')
    def test_export_schedule_excel(self, mock_connect, mock_read_sql_query, mock_to_csv, mock_to_excel, mock_showerror, mock_showwarning, mock_showinfo, mock_asksaveasfilename):
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_df = MagicMock()
        mock_df.empty = False
        mock_read_sql_query.return_value = mock_df
        mock_asksaveasfilename.return_value = 'output.xlsx'

        # Call the function
        main.export_schedule()

        # Assertions
        mock_connect.assert_called_once()
        mock_read_sql_query.assert_called_once_with('SELECT * FROM OPERATING_SCHEDULE', mock_conn)
        mock_conn.close.assert_called_once()
        mock_to_excel.assert_called_once_with('output.xlsx', index=False)
        mock_showinfo.assert_called_once()
        mock_showwarning.assert_not_called()
        mock_showerror.assert_not_called()

    @patch('main.filedialog.asksaveasfilename')
    @patch('main.messagebox.showinfo')
    @patch('main.messagebox.showwarning')
    @patch('main.messagebox.showerror')
    @patch('main.pd.DataFrame.to_excel')
    @patch('main.pd.DataFrame.to_csv')
    @patch('main.pd.read_sql_query')
    @patch('main.sqlite3.connect')
    def test_export_schedule_csv(self, mock_connect, mock_read_sql_query, mock_to_csv, mock_to_excel, mock_showerror, mock_showwarning, mock_showinfo, mock_asksaveasfilename):
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_df = MagicMock()
        mock_df.empty = False
        mock_read_sql_query.return_value = mock_df
        mock_asksaveasfilename.return_value = 'output.csv'

        # Call the function
        main.export_schedule()

        # Assertions
        mock_connect.assert_called_once()
        mock_read_sql_query.assert_called_once_with('SELECT * FROM OPERATING_SCHEDULE', mock_conn)
        mock_conn.close.assert_called_once()
        mock_to_csv.assert_called_once_with('output.csv', index=False)
        mock_showinfo.assert_called_once()
        mock_showwarning.assert_not_called()
        mock_showerror.assert_not_called()

    @patch('main.filedialog.asksaveasfilename')
    @patch('main.messagebox.showinfo')
    @patch('main.messagebox.showwarning')
    @patch('main.messagebox.showerror')
    @patch('main.pd.read_sql_query')
    @patch('main.sqlite3.connect')
    def test_export_schedule_empty(self, mock_connect, mock_read_sql_query, mock_showerror, mock_showwarning, mock_showinfo, mock_asksaveasfilename):
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_df = MagicMock()
        mock_df.empty = True
        mock_read_sql_query.return_value = mock_df
        mock_asksaveasfilename.return_value = 'output.xlsx'

        # Call the function
        main.export_schedule()

        # Assertions
        mock_showwarning.assert_called_once()
        mock_showinfo.assert_not_called()
        mock_showerror.assert_not_called()

if __name__ == '__main__':
    unittest.main()
