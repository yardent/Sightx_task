from google.oauth2 import service_account


class BQ_Client:

    API_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPE = ['https://www.googleapis.com/auth/drive']

    def __init__(self, service_key_location, project):
        self.credentials = service_account.Credentials.from_service_account_file(
            service_key_location,
        )
        self.project=project

    def load_df(self, df, table):
        df.to_gbq(table, project_id=self.project, if_exists='append',
                  progress_bar=True, credentials=self.credentials)