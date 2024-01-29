# filename_plugin.py
import os

def get_file_name(file_path):
    file_name_with_extension = os.path.basename(file_path)
    file_name_without_extension = os.path.splitext(file_name_with_extension)[0]
    return file_name_without_extension
