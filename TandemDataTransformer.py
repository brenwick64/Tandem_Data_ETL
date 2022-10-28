from datetime import datetime, timedelta
import pandas as pd
from configs import report_config

class TandemDataTransformer:
    """
    Class for transforming denormalized event data into usable 
    bolus, cgm, and calibration events.
    """
    
    def __init__(self):
        """
        Constructor for TandemDataTransformer - 
        pulls data from configuration file and sets instance variables
        """
        self.hour_gap = report_config.hour_gap
        self.bolus_config = report_config.bolus_events
        self.cgm_config = report_config.cgm_events
        self.calibration_config = report_config.calibration_events
        self.src_datetime_format = report_config.src_datetime_format
        self.tgt_datetime_format = report_config.tgt_datetime_format
    
    # Helper Functions
        
    def round_datetime(self, timestamp_string):
        """
        Rounds the input datetime value to the nearest floored 5 minute interval
        
        :params
            timestamp_string : str                 The timestamp value to be rounded                                
            
        :returns
            (floored_timestamp + delta): str       The new rounded timestamp string      
        """
        timestamp = datetime.strptime(timestamp_string, self.src_datetime_format)
        floored_timestamp = timestamp.replace(microsecond=0, second=0, minute=0, hour=timestamp.hour)
        delta = timedelta(minutes=timestamp.minute//5) * 5
        return str(floored_timestamp + delta)
    
    def add_hours_to_rounded_datetime(self, timestamp_string, hours):
        """
        Adds timedelta hours to an existing datetime to generate a new timestamp
        
        :params
            timestamp_string: str                  The timestamp to have hour(s) added
            
        :returns
            (timestamp + delta): str               The timestamp with the added hours                     
        """
        timestamp = datetime.strptime(timestamp_string, self.tgt_datetime_format)
        delta = timedelta(hours=hours)
        return str(timestamp + delta)   
    
    def append_joined_column(self, df1, df2, left_on, right_on, old_column_name, new_column_name):
        """
        Appends a single column to an existing dataframe using a pandas merge statement
        
        :params
            df1: dataframe                          The source dataframe
            df2: dataframe                          The target dataframe
            left_on: str                            The column to join on from the source dataframe
            right_on: str                           The column to join on from the target dataframe
            old_column_name:                        The name of the right dataframe's column to append
            new_column_name:                        The new name of the appended column 
            
        :returns
            joined_data: dataframe                  The new dataframe with the new column appended
        """
        new_columns = df1.columns.tolist() + [new_column_name]
        joined_data = pd.merge(df1, df2, left_on=left_on, right_on=right_on, suffixes=('', '_y'))    
        joined_data = joined_data.rename(columns={old_column_name : new_column_name})
        return joined_data[new_columns]
    
    def append_hourly_cgm_data(self, bolus_df, cgm_df, hours):
        """
        Appends a rounded timestamp column and a glucose reading column for each hour
        
        :params
            bolus_df: dataframe                     The source dataframe containing bolus events
            cgm_df: dataframe                       The target dataframe containing cgm events
            hours: int                              The number of hours to append cgm data
            
        :returns
            bolus_df: dataframe                     The new dataframe with all columns appended
        """
        for i in range(hours):
            bolus_df[f'{i+1}_hour_later_rounded_datetime'] = bolus_df.rounded_datetime.apply(self.add_hours_to_rounded_datetime, hours=i+1)
            bolus_df = self.append_joined_column(bolus_df, cgm_df, left_on=f'{i+1}_hour_later_rounded_datetime', 
                                            right_on='rounded_datetime', old_column_name='egv.estimatedGlucoseValue', 
                                            new_column_name=f'bg_{i+1}_hours')
        return bolus_df
    
    def split_denormalized_dataframe(self, df):
        """
        Splits a single denormalized dataframe into its three main event components: bolus, cgm, and calibration
        
        :params
            df: dataframe                           The main dataframe to be split
            
        :returns
            bolus_events: dataframe                 A new dataframe containing only events of type 'Bolus'
            cgm_events: dataframe                   A new dataframe containing only events of type 'CGM'
            calibration_events: dataframe           A new dataframe containing only events of type 'BG'
        """
        bolus_events = df.loc[df['type'] == 'Bolus'][self.bolus_config['columns']]
        cgm_events = df.loc[df['type'] == 'CGM'][self.cgm_config['columns']]
        calibration_events = df.loc[df['type'] == 'BG'][self.calibration_config['columns']]
        return bolus_events, cgm_events, calibration_events
    
    def generate_bolus_report(self, df):
        """
        Uses the above helper functions to generate a full bolus event dataframe with added columns from cgm events
        
        :params
            df: dataframe                           The main dataframe returned from TandemScraper
            
        :returns
            master_bolus_events: dataframe          The full bolus event dataframe
        """
        # Normalize event entries into 3 categories
        raw_bolus, raw_cgm, raw_calibration = self.split_denormalized_dataframe(df)
        
        # Add rounded timestamp to join other data on 
        raw_bolus['rounded_datetime'] = raw_bolus.eventDateTime.apply(self.round_datetime)
        raw_cgm['rounded_datetime'] = raw_cgm.eventDateTime.apply(self.round_datetime)
        raw_calibration['rounded_datetime'] = raw_calibration.eventDateTime.apply(self.round_datetime)
        
        # Appends hourly BG values onto bolus events
        master_bolus_events = self.append_hourly_cgm_data(raw_bolus, raw_cgm, self.hour_gap)
        
        return master_bolus_events