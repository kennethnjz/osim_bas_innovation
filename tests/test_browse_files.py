import unittest
from unittest.mock import patch, MagicMock

# Import your browse_files function from main.py
import main

class TestBrowseFiles(unittest.TestCase):
    @patch('main.filedialog.askopenfilename')
    @patch('main.filepath')
    @patch('main.pd.read_excel')
    @patch('main.pd.read_csv')
    @patch('main.sqlite3.connect')
    @patch('main.Path')
    def test_browse_files_excel(
        self, mock_path, mock_connect, mock_read_csv, mock_read_excel, mock_filepath, mock_askopenfilename
    ):
        # Setup mocks
        mock_askopenfilename.return_value = 'test.xlsx'
        mock_path.return_value.suffix = '.xlsx'
        mock_df = MagicMock()
        mock_read_excel.return_value = mock_df
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Call the function
        main.browse_files()

        # Assertions
        mock_askopenfilename.assert_called_once()
        mock_filepath.config.assert_called_once_with(text="File Opened: test.xlsx")
        mock_read_excel.assert_called_once()
        mock_df.to_sql.assert_called_once_with('OPERATING_SCHEDULE', mock_conn, if_exists='replace', index=False)
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
