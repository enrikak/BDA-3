import pandas as pd
import tkinter as tk
from tkinter import filedialog
import os


def select_file():
    """
    Opening a file dialog to select a file and return the file path.
    """
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(title="Select file",
                                           filetypes=(("CSV files", "*.csv"), ("All files", "*.*")))
    return file_path


def main():
    # Prompt the user to select a file
    file_path = select_file()

    if file_path:
        # Reading the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Printing the first few rows of the DataFrame for preview
        print(df.head())

        # Saving the DataFrame as "data.csv" in the current directory
        current_directory = os.getcwd()
        save_path = os.path.join(current_directory, 'data.csv')
        df.to_csv(save_path, index=False)

        print(f"File saved as {save_path}")
    else:
        print("No file selected.")


if __name__ == '__main__':
    main()
