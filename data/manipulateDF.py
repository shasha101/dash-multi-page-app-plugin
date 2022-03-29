import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np



def filter_data(dfx, dfx_dateCol, start_date, end_date):
    mask = (
            (dfx_dateCol.dt.date >= pd.to_datetime(start_date))     # pass .dt.date to filter dates
            & (dfx_dateCol.dt.date <= pd.to_datetime(end_date))
        )# daily sgym users

    dfx = dfx.loc[mask, :].reset_index(drop=True)

    return dfx


# groupby dataframe with datetime
def groupbyDF(dfx, date_key, freq, subkeys, agg_dict):
    # main datetime grouper
    grouper_list = [(pd.Grouper(key=date_key, freq=freq))]
    
    # sub groups
    for i in subkeys:
        grouper_list.append(pd.Grouper(key=i))
    
    # sort values in ascending order i.e. older dates first
    dfx = dfx.groupby(grouper_list).agg(agg_dict).reset_index().sort_values(by=date_key, ascending=True).reset_index(drop=True)

    return dfx, date_key, subkeys

# groupby dataframe without datetime
def groupbyDFnoDate(dfx, groupby_key, subkeys, agg_dict):
    # main datetime grouper
    grouper_list = [(pd.Grouper(key=groupby_key))]
    
    # sub groups
    for i in subkeys:
        grouper_list.append(pd.Grouper(key=i))
    
    # sort values in ascending order i.e. older dates first
    dfx = dfx.groupby(grouper_list).agg(agg_dict).reset_index().reset_index(drop=True)

    return dfx, groupby_key, subkeys
    

def countUniqueUsers(dfx, groupby_key, sort_key):
    # change value to 1 if value != 0
    dfx['activeSgId'] = dfx['activeSgId'].map(lambda x: 1 if x != 0 else x)
    # regroupby to sum up unique users
    dfx = dfx.groupby(groupby_key)['activeSgId'].agg('sum').reset_index()
    # sort by date in ascending order
    dfx = dfx.sort_values(by=sort_key).reset_index(drop=True)

    return dfx