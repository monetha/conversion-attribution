#!/usr/bin/env python3 

import pandas as pd
import numpy as np
import os
import sqlalchemy
import argparse
from dotenv import load_dotenv

from utils.functools import applyParallel
from utils.conv_atr import make_proxy_chains, slice_data,mark_chain
from user_journey import DataPreparer, SessionMarker

pd.options.mode.chained_assignment = None

DAYS_BEFORE_PROXY_DEFAULT = 30

parser = argparse.ArgumentParser()
parser.add_argument('--account_id', type=int, required=True)
parser.add_argument('--start_date_uj', type=str, required=True,
                    help="Start date of sessions for user journey markup")
parser.add_argument('--start_date', type=str, required=True,
                    help="Start date of sessions for conversion attribution markup")
parser.add_argument('--end_date', type=str, required=True,
                    help="End date of sessions for conversion attribution markup")
parser.add_argument('--proxy_days', type=int, required=False,
                    default=DAYS_BEFORE_PROXY_DEFAULT,
                    help="Number of days before proxy session is valid. Default is 30.")

args = parser.parse_args()

account_id = args.account_id
start_date = args.start_date
end_date = args.end_date
start_date_user_journey = args.start_date_uj
DAYS_BEFORE_PROXY = args.proxy_days

load_dotenv('.env')


def load_engine(base_name: str, engine: str, execution_options: dict):
    host = os.getenv(f'{base_name}_DB_HOST')
    db = os.getenv(f'{base_name}_DB_NAME')
    user = os.getenv(f'{base_name}_DB_USER')
    password = os.getenv(f'{base_name}_DB_PASSWORD')
    port = os.getenv(f'{base_name}_DB_PORT')
    connection_str = f'{engine}://{user}:{password}@{host}:{port}/{db}'
    return sqlalchemy.create_engine(connection_str, execution_options={})



def main(): 
    engine = load_engine('EVENTS', 'postgresql', {"stream_results": True})
    engine_save = load_engine('DS', 'postgresql', None)

    query = open('sql/user-journey.sql', 'r').read()

    sessions = pd.read_sql(query,
                        engine,
                        params={"start_date": start_date_user_journey,
                                "end_date": end_date, 'account_id': account_id}
                        )

    sessions_slice = sessions[(sessions['session_start'] >= start_date)
                    & (sessions['session_start'] < end_date)]
    
    coversion_users = sessions_slice[sessions_slice.is_buy_session ==
                                1]['customer_profile_id'].unique()
    
    sessions = sessions[sessions['customer_profile_id'].isin(coversion_users)]

    #Prepare user journey phases
    sessions = DataPreparer.process(
        sessions=sessions, groupby_id='customer_profile_id')

    sm = SessionMarker(356)
    sessions = sm.mark_bounce(sessions)
    sessions = sessions.assign(session_type=None)
    sessions = applyParallel(sessions.groupby(
        'customer_profile_id'), sm.mark_by_group_id)


    #Slice sessions for matrix calculation
    sessions_conversion = sessions[(sessions['session_start'] >= start_date)
                    & (sessions['session_start'] < end_date)]
    sessions_conversion = sessions_conversion.assign(is_proxy=0)
    sessions_conversion = sessions_conversion.assign(chain=None)

    #process for conversion attribution

    sessions_conversion['session_end_shifted'] = sessions_conversion['session_end'].shift(1)
    sessions_conversion['interval_between_end'] = (sessions_conversion['session_start'] - sessions_conversion['session_end_shifted']).dt.days
    sessions_conversion.loc[sessions_conversion.interval_between < 0, 'interval_between'] = 0 


    sessions_conversion.utm_campaign.fillna('None', inplace=True)

    sessions_conversion['ses_num'] = sessions_conversion.groupby('customer_profile_id').cumcount() +1 
    sessions_conversion = sessions_conversion.assign(is_proxy=0)
    sessions_conversion = sessions_conversion.assign(chain=None)
    #Mark chains
    sessions_conversion = sessions_conversion.groupby('customer_profile_id').apply(make_proxy_chains,DAYS_BEFORE_PROXY)

    #Create table
    session_number_df = sessions_conversion.groupby(
        ['number_in_chain', 'session_type']).size().unstack()
    session_number_df = session_number_df.rename(
        columns=sm._SessionMarker__inverse_map)
    session_number_df['total'] = session_number_df.sum(axis=1)
    session_number_df = session_number_df.assign(account_id=account_id)

    session_number_df.to_sql('journey_attribution',
                            con=engine_save, if_exists='append', schema='data')

if __name__ == "__main__": 
	main() 
