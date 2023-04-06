from numpy import arange
from pandas import Timedelta, DataFrame, concat

from .functools import current_next


def get_groups(df: DataFrame, days_before_proxy: int) -> list:
    '''
    Make sub chain sequence in sessions before proxy 
    '''
    if df.shape[0] == 1:
        return [df.ses_num.values]
    if df[df['interval_between'] > days_before_proxy].shape[0] == 0:
        return [df.ses_num.values]
    groups = [[] for i in range(df.shape[0])]
    group_id = 0
    for row in df.iterrows():
        groups[group_id].append(row[1]['ses_num'])
        if row[1]['interval_between'] >= days_before_proxy:
            group_id += 1

    return list(filter(None, groups))


def mark_chain(df: DataFrame, days_before_proxy: int):
    '''
    Make proxy chains and mark each session with their session number in a chain sequence. 
    '''
    current_chain_index = 0
    # user_id = df['customer_profile_id'].iloc[0]
    if df.shape[0] == 1:
        # df['chain'] = str(user_id) + '_' + str(current_chain_index)
        df['number_in_chain'] = 1
        return df

    atb_index = [0] + list(df[df.is_buy_session == 1].ses_num.values)
    if atb_index[-1] != df.ses_num.max():
        atb_index.append(df.ses_num.max())

    for current_index, next_index in current_next(atb_index):
        if next_index is None:
            break

        mask = (df.ses_num > current_index) & (df.ses_num <= next_index)
        df_temp = df[mask]

        for index_group in get_groups(df_temp, days_before_proxy):
            # df.loc[df.ses_num.isin(index_group),'chain'] = str(user_id) + '_' + str(current_chain_index)
            df.loc[df.ses_num.isin(index_group), 'number_in_chain'] = arange(
                1, len(index_group)+1)
            current_chain_index += 1

    return df


def slice_data(df: DataFrame, days_before_proxy: int):
    '''
    Mark proxy sessions
    '''
    df['ses_num'] = arange(1, df.shape[0]+1)
    if df[df.is_buy_session == 1].shape[0] == 0:
        return df
    for index, atb_session in df[df.is_buy_session == 1].iterrows():
        time_before = atb_session.session_start - \
            Timedelta(days=days_before_proxy)
        df_temp = df[(df['session_start'] > time_before)
                     & (df.ses_num < atb_session.ses_num)]
        df_temp = df_temp[df_temp.is_buy_session == 0]
        if df_temp.shape[0] == 0:
            continue
        df.loc[df_temp.index, 'is_proxy'] = 1
    return df


def make_proxy_chains(df: DataFrame, days_before_proxy: int):
    '''
    Sets proxy sessions and marks chain numbers.
    '''
    
    if days_before_proxy == 0:
        for atb_session in df[df.is_buy_session == 1].index:
            df.loc[df.index <= atb_session,'chain'] = df.loc[df.index <= atb_session,'chain'] + 1
    else:
        current_chain_number = 0
        atb_index = [0] + list(df[df.is_buy_session == 1].ses_num.values)
        for current_index, next_index in current_next(atb_index):
            if next_index is None:
                break

            df_temp = df[(df.ses_num > current_index) & (df.ses_num <= next_index)]
            time_before = df_temp.iloc[-1].session_start - Timedelta(days=days_before_proxy)

            for index_group in get_groups(df_temp, days_before_proxy):
                df.loc[df.ses_num.isin(index_group), 'number_in_chain'] = arange(
                    1, len(index_group)+1)
                df.loc[df.ses_num.isin(index_group), 'chain'] = current_chain_number
                current_chain_number+=1

            df_temp_proxy_numbers = df_temp[(df_temp['session_start'] > time_before)].iloc[:-1]['ses_num']
            df.loc[df['ses_num'].isin(df_temp_proxy_numbers),'is_proxy'] = 1


        df_after_proxy_chain = df[df.ses_num > atb_index[-1]]
        if df_after_proxy_chain.shape[0]:

            for index_group in get_groups(df_after_proxy_chain, days_before_proxy):
                df.loc[df.ses_num.isin(index_group), 'number_in_chain'] = arange(
                    1, len(index_group)+1)
                df.loc[df.ses_num.isin(index_group), 'chain'] =current_chain_number
                current_chain_number+=1
    return df

def make_same_column(heatmap_data):
    new_data = []
    for i in heatmap_data.index:
        try:
            new_data.append(heatmap_data.loc[i,i])
        except:
            new_data.append(0)
    heatmap_data['same_source'] = new_data

    heatmap_data = concat([heatmap_data[['same_source']], heatmap_data.drop(columns = ['same_source'])],axis=1)
    return heatmap_data