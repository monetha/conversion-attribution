import pandas as pd

def get_events_before_target(df, col, start, stop):
    mask = ~(df[col]==start).cummax() ^ (df[col]==stop).cummax()
    return df[mask]


class SessionMarker():
    def __init__(self,interval_max = 365, min_events_number = 10, min_session_length = 20, min_page_view_number = 1):
        self.buy_operations = ['add_to_basket','place_order','go_to_checkout']
        self.interval_max = interval_max
        self.min_events_number = min_events_number
        self.min_session_length = min_session_length
        self.min_page_view_number = min_page_view_number
        self.__mark_map = {False : 'Awareness', True : 'Consideration'}
        
        self.__annotation_map = {'Undefined' : 1, 
                                 'Awareness' : 2, 
                                 'Consideration' : 3, 
                                 'Acquisition' : 4, 
                                 'Service' : 5,
                                 'Loyalty' : 6,
                                 'Loyalty+' : 7
                                }
        self.__inverse_map  = {v: k for k, v in self.__annotation_map.items()}
        

    def check_if_bounce_by_lenght(self, session_data):
        return (session_data.events_number < self.min_events_number) \
        & ((session_data.session_end - session_data.session_start).seconds < self.min_session_length)

    
    def check_if_bounce_by_page(self, session_data):
        return (session_data.page_views_count <= self.min_page_view_number)\
        & (session_data.is_name_action_type == 0)\
        & (session_data.is_buy_session == 0)

    
    def remark_buy_sessions(self, sessions):
        sessions['is_buy_session_marked'] = sessions['is_buy_session']
        sessions.loc[(sessions.is_buy_session == 1) & (sessions.is_bounce == True), 'is_buy_session_marked'] = 0
        return sessions
    

    def mark_by_group_id(self,df, to_map = False):
        """
        Method for marking `session_type` many sessions represented as type `pd.DataFrane` 
        with previously called methods `SessionMarker.mark_bounce`.
        
        
        DataFrame must include following attribues: 
        `is_bounce` : precalculeted field from method `SessionMarker.mark_bounce`
        `is_buy_session` : 
                1 if session has any of listed events ['add_to_basket', 'go_to_checkout', 'place_order']
                0 if session has NO any of listed events ['add_to_basket', 'go_to_checkout', 'place_order']
        `attention_span_mark` : precalculeted field from method `SessionMarker.cal_attention_score`
        `events_number` : number of session events
        
        Parameters
        ----------
        data : pd.DataFrame
        
        Returns
        -------
            
        pd.DataFrame
            DataFrame with marked session type in field 'session_type'
            
        """        

        for index_group in self.get_groups(df): 
            
            temp_cons = get_events_before_target(df[(df.number.isin(index_group))],'is_buy_session',None,True)

            df.loc[(df.number.isin(temp_cons.number.values))\
                   & ((df.attention_span_mark == True) & (df.events_number > self.min_events_number)),\
                   'session_type'] = 3
        
            df.loc[(df.number.isin(temp_cons.number.values)) \
                   & ((df.events_number <= self.min_events_number) | (df.attention_span_mark == False)),\
                   'session_type'] = 2
            
            buy_sessions = df[(df.number.isin(index_group)) & (df.is_buy_session == True) ]
            
            if buy_sessions.shape[0] != 0:
                first_buy_session = buy_sessions.iloc[0]
                df.loc[df.number == first_buy_session.number,'session_type'] = 4
            if len(index_group) == 1:
                df.loc[(df.number.isin(index_group)) & (df.is_bounce == True), 'session_type'] = 1
#                 df.loc[(df.number.isin(index_group)),'session_type'] = df[(df.number.isin(index_group))]['session_type'].map(self.__annotation_map)
                continue

            df.loc[(df.number.isin(index_group)) & (df.is_bounce == True) , 'session_type'] = 1
            temp_service = get_events_before_target(df[(df.number.isin(index_group))],'is_buy_session',True,False)
            if temp_service.shape[0] != df[(df.number.isin(index_group))].shape[0]:
                temp_service = temp_service[(temp_service.is_buy_session == False)]
                df.loc[df.number.isin(temp_service.number), 'session_type'] = 5
            
            
            if buy_sessions.shape[0] > 1:
                second_buy_session = buy_sessions.iloc[1]
                df.loc[df.number == second_buy_session.number, 'session_type'] = 6
                index_loyal_plus = list(index_group).index(second_buy_session.number)
                if index_loyal_plus + 1 != len(index_group):
                    df.loc[df.number.isin(index_group[index_loyal_plus + 1:]), 'session_type'] = 7
                    
            
#             df.loc[(df.number.isin(index_group)),'session_type'] = df[(df.number.isin(index_group))]['session_type'].map(self.__annotation_map)
        
            sequence = df[(df.number.isin(index_group))]['session_type'].values
            
            new_seq = [sequence[0]]
            for value in sequence[1:]:
                if value <= new_seq[-1]:
                    new_seq.append(new_seq[-1])
                else:
                    new_seq.append(value)
                    
            df.loc[(df.number.isin(index_group)),'session_type'] = new_seq
            
        if to_map:
            df.session_type = df.session_type.map(self.__inverse_map)
        return df

    
    def get_groups(self,df):
        if df[df['interval_between'] > self.interval_max].shape[0] == 0:
            return [df.number.values]
        groups = [[]]
        group_id = 0
        for row in df.iterrows():
            if row[1]['interval_between'] >= self.interval_max:
                group_id += 1
                groups.append([])
            groups[group_id].append(row[1]['number'])
            
        return groups 
    
 
    def mark_bounce(self,data, bounce_type = 'page'):
        '''
        Parameters
        ----------
        bounce_type : str 
            avaliable types = ['page', 'lenght']
            
            
        Returns
        -------
        pd.DataFrame
            DataFrame with marked undefined sessions in field 'is_bounce'
            
        pd.Series
            Series with marked undefined sessions in field 'is_bounce'
            
        Raises
        ------
        Exception
            If data field is not instance of `pd.DataFrame` or `pd.Series`
        '''
        
        if isinstance(data,pd.DataFrame):
            data['is_bounce'] = False    
            data.loc[getattr(self,f'check_if_bounce_by_{bounce_type}')(data), 'is_bounce'] = True
            return data
        elif isinstance(data,pd.Series):
        
            data['is_bounce'] = False
            data['is_bounce'] = getattr(self,f'check_if_bounce_by_{bounce_type}')(data)
            return data
        
        raise Exception('Invalid data type. Avaliable - ["pd.DataFrame","pd.Series"]')

    
    def mark_single_session(self, session, prev_type, prev_session_end):
        """
        Method for marking `session_type` a single session represented as type `pd.Series` 
        with previously called methods `SessionMarker.mark_bounce` , `SessionMarker.cal_attention_score`.
        
        if difference between prev_session_end and session_start is more than 356 the rule of sequence will not be applied.
        
        DataFrame must include following attribues: 
        `is_bounce` : precalculeted field from method `SessionMarker.mark_bounce`
        `is_buy_session` : 
                1 if session has any of listed events ['add_to_basket', 'go_to_checkout', 'place_order']
                0 if session has NO any of listed events ['add_to_basket', 'go_to_checkout', 'place_order']
        `attention_span_mark` : precalculeted field from method `SessionMarker.cal_attention_score`
        `events_number` : number of session events
        
         Parameters
        ----------
        data : pd.Series
        
        prev_type : str
            Type of previous session
        
        prev_session_end : datetime
            Datetime of previous session end
        
        Returns
        -------
            
        pd.Series
            Series with marked session type in field 'session_type'
            
        """
        if prev_session_end:
            prev_type = prev_type if (prev_session_end - session['session_start'] ).days < 356 else 1
        else:
            prev_type = 1
        
        new_type = None
        if prev_type <= 3:
            if session['is_bounce']:
                new_type = 1
            elif session['is_buy_session']:
                new_type = 4
            elif session['attention_span_mark'] and session['events_number'] > self.min_events_number:
                new_type = 3
            elif session['attention_span_mark'] == False or session['events_number'] <= self.min_events_number:
                new_type = 2
                
            new_type = prev_type if new_type < prev_type else new_type
        elif prev_type < 6:
            new_type = 6 if session['is_buy_session'] else 5
        else:
            new_type = 7
          
        session.at['session_type'] = new_type 
        return session
