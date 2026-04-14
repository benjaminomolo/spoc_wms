import os

from flask import current_app

# Function to check allowed file extensions

def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in current_app.config['ALLOWED_EXTENSIONS']


# Function to check if file already exists
def file_exists(file_path):
    return os.path.isfile(file_path)


# Function to generate a new filename if a file with the same name exists
def generate_unique_filename(folder_path, filename):
    base, extension = os.path.splitext(filename)
    counter = 1
    new_filename = filename
    while file_exists(os.path.join(folder_path, new_filename)):
        new_filename = f"{base}_{counter}{extension}"
        counter += 1
    return new_filename


# Define the maximum file size (e.g., 1 MB)
MAX_FILE_SIZE = 1 * 1024 * 1024  # 5 MB


# Function to check if the file size is within the allowed limit
def is_file_size_valid(file):
    return file.content_length is not None and file.content_length <= MAX_FILE_SIZE
