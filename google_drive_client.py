

import google
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pandas as pd
import os
import requests
import json
import io
from googleapiclient.http import MediaIoBaseDownload


class Google_Drive_Client:

    API_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPE = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.readonly']


    def __init__(self, service_key_location):
        self.service_key_location=service_key_location
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            service_key_location, scopes=self.SCOPE)

        self.service = build(self.API_NAME, self.API_VERSION, credentials=self.credentials)


    def get_all_folders(self, all_folder_dict, folder_id):


        folder_dict = self.get_folders(folder_id)
        if(len(folder_dict) >0):
            all_folder_dict.update(folder_dict)
            for name in folder_dict:
                all_folder_dict.update(self.get_all_folders(all_folder_dict, folder_dict[name]))
        return all_folder_dict

    def get_all_files(self, folder_id, file_type):
        df = self.get_data_from_drive(folder_id)
        all_file = []

        for key, row in df.iterrows():
            if file_type in row['mimeType']:
                file_name = row['name']
                file_id = row['id']
                all_file.append(file_name)
                request = self.service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fd=fh, request=request)

                done = False

                while not done:
                    status, done = downloader.next_chunk()

                fh.seek(0)

                with open(file_name, 'wb') as f:
                    f.write(fh.read())
                    f.close()
        return all_file

    def get_folders(self, folder_id):
        df = self.get_data_from_drive(folder_id)
        folder_dict = {}

        for key, row in df.iterrows():
            if 'folder' in row['mimeType']:
                folder_dict[row['name']] = row['id']

        return folder_dict

    def get_data_from_drive(self, folder_id):
        page_token = None
        query = f"parents= '{folder_id}'"
        response = self.service.files().list(q=query,
                                             spaces='drive',
                                             pageToken=page_token).execute()
        files = response.get('files')
        nextPageToken = response.get('nextPageToken')
        while nextPageToken:
            response = self.service.files().list(q=query,
                                                 spaces='drive',
                                                 fields='nextPageToken, files(kind, id, name, mimeType)',
                                                 pageToken=page_token).execute()
            files.extend(response.get('files'))
            nextPageToken = response.get('nextPageToken')

        df = pd.DataFrame(files)

        return df