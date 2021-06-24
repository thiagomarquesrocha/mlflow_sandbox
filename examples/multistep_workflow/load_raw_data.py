"""
Downloads the Bug Report dataset and saves it as an artifact
"""
import requests
import tempfile
import os
import zipfile
import pyspark
import mlflow
import click
from google_drive_downloader import GoogleDriveDownloader as gdd

#taken from this StackOverflow answer: https://stackoverflow.com/a/39225039
import requests

def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)    

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

@click.command(
    help="Downloads the Bug Report dataset and saves it as an mlflow artifact "
    " called 'bug report'."
)
@click.option("--url", default="https://drive.google.com/u/0/uc?export=download&confirm=0DAi&id=1reRGkmSItk0MJyiefbIjEAEfujAg7JDk")
def load_raw_data(url):
    with mlflow.start_run() as mlrun:
        local_dir = tempfile.mkdtemp()
        local_filename = os.path.join(local_dir, "dataset.zip")
        print("Downloading %s to %s" % (url, local_filename))
        file_id = '1aGJNh3T7KT7TdFFca3HOuE8Yxmxumrhl'
        # TODO: Download from google drive
        gdd.download_file_from_google_drive(file_id=file_id,
                                   dest_path=local_filename,
                                   unzip=True)

        extracted_dir = os.path.join(local_dir, "bug-report")
        print("Extracting %s into %s" % (local_filename, extracted_dir))
        with zipfile.ZipFile(local_filename, "r") as zip_ref:
           zip_ref.extractall(local_dir)

        print("Uploading datasets: %s" % extracted_dir)
        mlflow.log_artifact(extracted_dir)


if __name__ == "__main__":
    load_raw_data()