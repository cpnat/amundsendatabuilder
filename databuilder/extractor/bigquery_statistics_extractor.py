import json
import google.oauth2.service_account
from databuilder.extractor.base_extractor import Extractor

class BigQueryStatisticsExtractor(Extractor):

    """
    A column profiler for bigquery tables.
    """

    PROJECT_ID_KEY = 'project_id'
    KEY_PATH_KEY = 'key_path'
    _DEFAULT_SCOPES = ['https://www.googleapis.com/auth/bigquery.readonly', ]

    def init(self, conf):
        self.project_id = conf.get_string(BigQueryStatisticsExtractor.PROJECT_ID_KEY)
        self.key_path = conf.get_string(BigQueryStatisticsExtractor.KEY_PATH_KEY, None)


        if self.key_path:
            credentials = (
                google.oauth2.service_account.Credentials.from_service_account_file(
                    self.key_path, scopes=BigQueryStatisticsExtractor._DEFAULT_SCOPES))
        else:
            if self.cred_key:
                service_account_info = json.loads(self.cred_key)
                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_info(
                        service_account_info, scopes=BigQueryStatisticsExtractor._DEFAULT_SCOPES))
            else:
                credentials, _ = google.auth.default(scopes=BigQueryStatisticsExtractor._DEFAULT_SCOPES)
