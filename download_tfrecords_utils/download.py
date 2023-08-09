from google_auth_oauthlib.flow import InstalledAppFlow
import io
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Define the scopes
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Obtain your Google credentials
def get_credentials():
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else: 
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    return creds

# Build the downloader
creds = get_credentials()
drive_downloader = build('drive', 'v3', credentials=creds)

def download_file(file_id, file_name, destination_folder):
    request = drive_downloader.files().get_media(fileId=file_id)
    fh = io.FileIO(os.path.join(destination_folder, file_name), mode='wb')
    print("Downloading " + f"{file_name}")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

# Replace 'FOLDER_ID' with your actual Google Drive folder ID
folder_id = '1vYrVrJgCPY4a8AlubnaIGojCqvhiK4I4'
query = f"Folder ID '{folder_id}'"
# results = drive_downloader.files().list(q=query, pageSize=1000,fields="nextPageToken, files(id, name)").execute()

def download_files_from_folder(folder_id_name, destination_folder):
    folder_id = folder_id_name[0]
    folder_name = folder_id_name[1]
    file_ids = []
    page_token = None
    while True:
        response = drive_downloader.files().list(
            q=f"'{folder_id}' in parents",
            spaces='drive',
            fields='nextPageToken, files(id, name)',
            pageToken=page_token
        ).execute()

        for file in response.get('files', []):
            file_id = file['id']
            file_name = file['name']
            subfolder_path = destination_folder + "/" + folder_name
            if not os.path.exists(subfolder_path):
                os.makedirs(subfolder_path)
            download_file(file_id, file_name, subfolder_path)

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return file_ids

def get_folder_names(folder_id):
    folders = []
    page_token = None
    while True:
        response = drive_downloader.files().list(
            q=f"'{folder_id}' in parents",
            spaces='drive',
            fields='nextPageToken, files(id, name)',
            pageToken=page_token
        ).execute()

        for file in response.get('files', []):
            file_id = file['id']
            file_name = file['name']
            folders.append((file_id, file_name))
            # download_file(file_id, file_name, destination_folder)

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return folders

destination_folder = "/deep/u/sjayyang/tfrecords/gdrive"
folders = get_folder_names(folder_id)
for folder in folders:
    download_files_from_folder(folder, destination_folder)
