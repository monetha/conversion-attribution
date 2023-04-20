#!/usr/bin/env python3 

import pandas as pd
import numpy as np
import os
import sqlalchemy
import argparse
from dotenv import load_dotenv

from utils.conv_atr import make_proxy_chains, make_same_column


pd.options.mode.chained_assignment = None

DAYS_BEFORE_PROXY_DEFAULT = 30

parser = argparse.ArgumentParser()
parser.add_argument('--account_id', type=int, required=True)
parser.add_argument('--start_date', type=str, required=True,
                    help="Start date of sessions_conversion for conversion attribution markup")
parser.add_argument('--end_date', type=str, required=True,
                    help="End date of sessions_conversion for conversion attribution markup")
parser.add_argument('--only_conversion', type=bool, required=True,
                    help="Count only conversion chains. Filters out the chains without conversion session in the end of the chain.")
parser.add_argument('--proxy_days', type=int, required=False,
                    default=DAYS_BEFORE_PROXY_DEFAULT,
                    help="Number of days before proxy session is valid. Default is 30.")


args = parser.parse_args()

account_id = args.account_id
start_date = args.start_date
end_date = args.end_date
only_conversion = args.only_conversion
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
    engine = load_engine('DATA', 'postgresql', {"stream_results": True})
    engine_save = load_engine('DS', 'postgresql', None)

    query = open('sql/source_medium.sql', 'r').read()

    sessions = pd.read_sql(query,
                        engine,
                        params={"start_date": start_date,
                                "end_date": end_date, 'account_id': account_id}
                        )

    
    coversion_users = sessions[sessions.is_buy_session ==
                                1]['customer_profile_id'].unique()
    
    sessions = sessions[sessions['customer_profile_id'].isin(coversion_users)]

    sessions_conversion = sessions.sort_values(by=['created']).reset_index(drop=True)


    #process for conversion attribution
    if DAYS_BEFORE_PROXY:
        sessions_conversion['session_end_shifted'] = sessions_conversion['session_end'].shift(1)
        sessions_conversion['interval_between'] = (sessions_conversion['session_start'] - sessions_conversion['session_end_shifted']).dt.days
        sessions_conversion.loc[sessions_conversion.interval_between < 0, 'interval_between'] = 0 


    sessions_conversion.utm_campaign.fillna('None', inplace=True)

    sessions_conversion['ses_num'] = sessions_conversion.groupby('customer_profile_id').cumcount() +1 
    sessions_conversion = sessions_conversion.assign(is_proxy=0)
    sessions_conversion = sessions_conversion.assign(chain=0)
    #Mark chains
    sessions_conversion = sessions_conversion.groupby('customer_profile_id').apply(make_proxy_chains,DAYS_BEFORE_PROXY)

    if only_conversion:
        sessions_conversion = sessions_conversion.groupby(['customer_profile_id', 'chain']).filter(lambda x : x.shape[0] != 1 )
        sessions_conversion = sessions_conversion.groupby(['customer_profile_id', 'chain']).filter(lambda x : x.is_buy_session.sum() > 0  )

    #Create heatmap

    sessions_size = sessions_conversion.groupby(['customer_profile_id','chain']).size().rename('session_count').reset_index()
    sessions_size['merge_index'] = sessions_size['customer_profile_id'].astype('str') + '_' + sessions_size['chain'].astype('str')
    sessions_size = sessions_size.set_index('merge_index')
    sessions_size = sessions_size.drop(columns=['customer_profile_id','chain'])

    first_chain_sessions = sessions_conversion.groupby(['customer_profile_id','chain']).head(1)[['customer_profile_id','chain','source_medium','utm_campaign']]
    first_chain_sessions['merge_index'] = first_chain_sessions['customer_profile_id'].astype('str') + '_' + first_chain_sessions['chain'].astype('str')
    first_chain_sessions['source_medium_campaign_first'] = (first_chain_sessions['source_medium'] + '/' + first_chain_sessions['utm_campaign']).str.lower()
    first_chain_sessions = first_chain_sessions.set_index('merge_index')
    first_chain_sessions = first_chain_sessions[['source_medium_campaign_first']]

    last_chain_sessions = sessions_conversion.groupby(['customer_profile_id','chain']).tail(1)[['customer_profile_id','chain','source_medium','utm_campaign']]
    last_chain_sessions['merge_index'] = last_chain_sessions['customer_profile_id'].astype('str') + '_' + last_chain_sessions['chain'].astype('str')
    last_chain_sessions['source_medium_campaign_last'] = (last_chain_sessions['source_medium'] + '/' + last_chain_sessions['utm_campaign']).str.lower()
    last_chain_sessions = last_chain_sessions.set_index('merge_index')
    last_chain_sessions = last_chain_sessions[['source_medium_campaign_last']]

    res_df = pd.concat([first_chain_sessions, last_chain_sessions,sessions_size],axis=1)

    heatmap_data = res_df.groupby(['source_medium_campaign_first','source_medium_campaign_last'])['session_count'].count().unstack()
    heatmap_data = make_same_column(heatmap_data)
    heatmap_data = heatmap_data.reset_index()

    heatmap_data.insert(loc=0, column='campaign', value = heatmap_data['source_medium_campaign_first'].apply(lambda x : x.split('/')[2]))
    heatmap_data.insert(loc=0, column='medium', value= heatmap_data['source_medium_campaign_first'].apply(lambda x : x.split('/')[1]))
    heatmap_data.insert(loc=0, column='source', value=heatmap_data['source_medium_campaign_first'].apply(lambda x : x.split('/')[0]))
    heatmap_data.drop('source_medium_campaign_first', inplace=True, axis=1)

    #TODO save to db instead file system
    # heatmap_data.to_sql('journey_attribution',
    #                         con=engine_save, if_exists='append', schema='data')

    if not os.path.exists('conversion_attribution_out'):
        os.mkdir('conversion_attribution_out')
    heatmap_data.to_csv(f'conversion_attribution_out/{account_id}_heatmap_campaign_count.csv')

if __name__ == "__main__": 
	main() 
