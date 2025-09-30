import requests
import base64
import os
import logging
from airflow.models import Variable

class accessToken:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client_ID = Variable.get("CLIENT_ID")
        self.client_secret = Variable.get("CLIENT_SECRET")
        self.url = 'https://accounts.spotify.com/api/token'

    def authentication(self):
        """Return base64 encoded client_id:client_secret"""
        self.logger.info("Encoding client credentials")
        auth_str = f"{self.client_ID}:{self.client_secret}"
        return base64.b64encode(auth_str.encode()).decode('utf-8')

    def request_access_token(self):
        """Client Credentials flow (no user login)"""
        self.logger.info("Requesting access token from Spotify API")

        try:
            auth_header = self.authentication()

            headers = {
                'Authorization': f'Basic {auth_header}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            data = {
                'grant_type': 'client_credentials'
            }

            self.logger.info("Making POST request to Spotify token endpoint")
            response = requests.post(self.url, headers=headers, data=data)

            if response.status_code == 200:
                token_data = response.json()
                access_token = token_data.get('access_token')

                if access_token:
                    self.logger.info("Access token successfully obtained")
                    return {
                        "access_token": access_token,
                        "expires_in": token_data.get("expires_in"),
                        "token_type": token_data.get("token_type")
                    }
                else:
                    self.logger.error("Access token not found in response")
                    return None
            else:
                self.logger.error(f"Failed to get access token. Status code: {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return None

        except Exception as e:
            self.logger.error(f"Exception occurred while requesting access token: {str(e)}")
            raise
