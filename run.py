
from TandemScraper import TandemScraper
from TandemDataTransformer import TandemDataTransformer
from S3BucketConnector import S3BucketConnector
import argparse
p = argparse.ArgumentParser()
p.add_argument('--todate')
p.add_argument('--fromdate')

def extract(scraper, to_date, from_date):
    """
    Uses TandemScraper class to extract dataframe of denormalized event data
    """ 
    # Collect Tandem Data
    session = scraper.login()
    userid, token = scraper.get_auth_credentials(driver=session)
    json_data = scraper.get_json_data(token=token, from_date=from_date, to_date=to_date, userid=userid)
    df = scraper.json_to_event_df(json_data=json_data)
    return df

def transform(transformer, df):
    """
    Uses TandemDataTransformer class to convert source dataframe to joined bolus events
    """
    df = transformer.generate_bolus_report(df)
    return df

def load(connector, df, to_date, from_date):
    """
    Uses S3BucketConnector class to upload transformed dataframe to target S3 bucket
    """
    connector.upload_df_to_s3(df=df, from_date=from_date, to_date=to_date)
    
def main(**kwargs):
    """
    Main function to run full ETL job
    """
    
    # Args
    to_date = kwargs['todate']
    from_date = kwargs['fromdate']
    
    # Initialize instanced objects
    scraper = TandemScraper()
    transformer = TandemDataTransformer()
    connector = S3BucketConnector()

    # Run ETL Job
    raw_df = extract(scraper=scraper, to_date=to_date, from_date=from_date)
    clean_df = transform(transformer=transformer, df=raw_df)
    load(connector=connector, df=clean_df, to_date=to_date, from_date=from_date)
     
if __name__ == '__main__':
    """
    Entrypoint for Tandem Data ETL job
    """
    args = p.parse_args()
    main(**vars(args))