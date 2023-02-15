from pandas import DataFrame

class DataPreparer:
    def __init__(self, groupby_id : str) -> None:
        self.groupby_id = groupby_id
    
    @staticmethod
    def process(sessions: DataFrame, groupby_id : str) -> DataFrame:
        sessions = sessions.sort_values(by=['session_start'])

        sessions['attention_span'] = (sessions['documents_mouse_out_count'] - sessions['documents_mouse_enter_count'])\
            * sessions[['documents_mouse_out_count','documents_mouse_enter_count']].max(axis=1)

        sessions['attention_span'] = sessions['attention_span'].fillna(0)
        sessions.loc[:,'attention_span_mark'] = None
        sessions['attention_span_mark'] = sessions['attention_span'].apply(lambda x : True if x != 0 else False)

        sessions['duration'] = sessions['session_end'] - sessions['session_start']
        sessions['duration'] = sessions['duration'].dt.total_seconds()

        sessions['number'] = sessions.groupby(groupby_id).cumcount()+1
        sessions['session_end_shifted'] = sessions.groupby(groupby_id)['session_end'].shift(1)
        sessions['interval_between'] = (sessions['session_start'] - sessions['session_end_shifted']).dt.days
        return sessions