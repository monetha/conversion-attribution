#!/usr/bin/env python3 

import pandas as pd
import numpy as np
import os
import sqlalchemy
import argparse
from dotenv import load_dotenv

from utils.functools import applyParallel
from utils.conv_atr import slice_data, mark_chain
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
    # CHANGED IN 'sql/user-journey.sql'
    # sessions['events_number'] = sessions['actions_count']


    #Prepare user journey phases
    sessions = DataPreparer.process(
        sessions=sessions, groupby_id='customer_profile_id')

    sm = SessionMarker(356)
    sessions = sm.mark_bounce(sessions)
    sessions['session_type'] = None
    res = applyParallel(sessions.groupby(
        'customer_profile_id'), sm.mark_by_group_id)


    #Slice sessions for matrix calculation
    res_sliced = res[(res['session_start'] >= start_date)
                    & (res['session_start'] < end_date)]
    res_sliced = res_sliced.assign(is_proxy=0)
    res_sliced = res_sliced.groupby('customer_profile_id', group_keys=False).apply(
        slice_data, DAYS_BEFORE_PROXY)

    #process for conversion attribution
    res_sliced['session_end_shifted'] = res_sliced.groupby('customer_profile_id')[
        'session_end'].shift(-1)
    res_sliced['session_start_shifted'] = res_sliced.groupby(
        'customer_profile_id')['session_start'].shift(-1)
    res_sliced['interval_between'] = (
        res_sliced['session_start_shifted'] - res_sliced['session_end']).dt.days
    res_sliced.loc[res_sliced['interval_between'] < 0, 'interval_between'] = 0

    coversion_users = res_sliced[res_sliced.is_buy_session ==
                                1]['customer_profile_id'].unique()
    atb_customers = res_sliced[res_sliced.customer_profile_id.isin(
        coversion_users
    )]
    atb_customers.utm_campaign.fillna('None', inplace=True)

    #Mark chains
    res_atb = atb_customers.groupby('customer_profile_id', group_keys=False).apply(
        mark_chain, DAYS_BEFORE_PROXY)

    #Create table
    session_number_df = res_atb.groupby(
        ['number_in_chain', 'session_type']).size().unstack()
    session_number_df = session_number_df.rename(
        columns=sm._SessionMarker__inverse_map)
    session_number_df['total'] = session_number_df.sum(axis=1)
    session_number_df = session_number_df.assign(account_id=account_id)

    session_number_df.to_sql('journey_attribution',
                            con=engine_save, if_exists='append', schema='data')


if __name__ == "__main__": 
	main() 
