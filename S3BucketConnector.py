import os
from datetime import datetime
from io import StringIO
import pandas as pd
import boto3


class S3BucketConnector:
    """
    Class for interacting with AWS S3 buckets
    """

    def __init__(self):
        """
        Constructor for S3BucketConnector -
        automatically connects to s3 bucket defined within the environment variables
        """
        self.s3 = boto3.resource('s3',
                               aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), 
                               aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY_ID')
                               )

        self.tgt_bucket = self.s3.Bucket(name=os.getenv('S3_TGT_BUCKET_NAME'))           
        
        
    def upload_df_to_s3(self, df, from_date, to_date):
        """
        uploads pandas dataframe to s3 bucket with from date and to date in the filename
        
        :params
            df : dataframe              the pandas dataframe to convert to csv
            from_date : str             the start date of the collected data
            to_date : str               the end date of the collected data
        """
        key = f'tandem-normalized-events_{from_date}_{to_date}.csv'
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        self.tgt_bucket.put_object(Key=key, Body=csv_buffer.getvalue())

        