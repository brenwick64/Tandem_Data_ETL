import os
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from dotenv import load_dotenv
load_dotenv()

class TandemScraper:
    """
    Class for connecting to Tandem TConnect URL and extracting data
    """
    
    def __init__(self):
        """
        Constructor for TandemScraper -
        sets object login credentials & urls to environment variable configurations
        """
        self.login_url = os.getenv('LOGIN_URL')
        self.base_api_url = os.getenv('API_URL')
        self.api_origin = os.getenv('API_ORIGIN')
        self.api_referrer = os.getenv('API_REFERRER')
        self.username = os.getenv('USERNAME')
        self.password = os.getenv('PASSWORD')
        
        
    def login(self):
        """
        Utilizes the selenium webdriver package to open the Tandem login page and
        injects the stored user credentials into the input fields
        
        :returns
            driver: webdriver.Chrome       The webdriver instance after a successful login
        """
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        s = Service('/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=s, options=chrome_options)
        driver.get(self.login_url)
        
        # Driver tries to find an element, if this fails, driver will wait X seconds then try again before failing
        driver.implicitly_wait(10)
        
        # Fill out login form
        driver.find_element('id', 'txtLoginEmailAddress').send_keys(self.username)
        driver.find_element('id', 'txtLoginPassword').send_keys(self.password)
        driver.find_element('id', 'linkLogin').click()
        
        return driver
    
    
    def get_auth_credentials(self, driver):
        """
        Extracts the needed authentication credentials from a logged-in webdriver session's cookies
        
        :params
            driver: webdriver.Chrome        The webdriver instance after a successful login
        
        :returns
            userid: str                     The GUID userid of the logged in user
            token: str                      The stored login token used to interact with the API
        """
        cookies = driver.get_cookies()
        for cookie in cookies:
            if cookie['name'] == 'accessToken':
                token = cookie['value']    
            elif cookie['name'] == 'UserGUID' :
                userid = cookie['value']
        return userid, token
    


    def get_json_data(self, token, from_date, to_date, userid):
        """
        Builds an API request using environment data & authentication credentials to fetch the user's data
        
        :params
            token: str                      The GUID userid of the logged in user
            from_date: str                  The start date of the data to be collected
            to_date: str                    The end date of the data to be collected
        
        :returns
            request.json(): serializable    The raw JSON data from a successful request
            
        """
        full_url = f'{self.base_api_url}/{from_date}/{to_date}/false?userid={userid}'
        
        request = requests.get(full_url, headers= {
        'Accept' : 'application/json',
        'Accept-Language' : 'en-US,en;q=0.7',
        'Authorization' : f'Bearer {token}',
        'Connection' : 'keep-alive',
        'Content-Type' : 'application/json',
        'Origin' : self.api_origin,
        'Referer' : self.api_referrer,
        'Sec-Fetch-Dest' : 'empty',
        'Sec-Fetch-Mode' : 'cors',
        'Sec-Fetch-Site' : 'same-site',
        'Sec-GPC' : '1',
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
        })
        
        return request.json()
    
    def json_to_event_df(self, json_data):
        """
        Extracts the event data as a dataframe from the json payload
        
        :params
            token: json_data: serializable              The raw JSON data from a successful request
        
        :returns
            pd.json_normalize(): dataframe              A pandas dataframe containing all event data 
        """
        return pd.json_normalize(json_data['event'])
        
        
