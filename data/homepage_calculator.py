import plotly.graph_objects as go
import pandas as pd
import numpy as np

from data import data_pipeline

import base64

import pytz, time
from datetime import datetime, tzinfo, timezone, timedelta, date

from data import manipulateDF
from figures import sgymOverview_figs


''' DATETIME NOW '''
localTimezone = pytz.timezone('Asia/Singapore')
datetimeNow = datetime.now(localTimezone)


# past n-days calculator. to be used with dfab where date = exercise_ended
def computePastDates(ndays, dfx, date_key, agg_key):
    
    date_list = []
    for i in list(range(1,(ndays+1))):
        date = datetimeNow.date() - timedelta(days=i)
        date_list.append(date)
    
    # generate last 60 entries from DB, data must be sorted by date in ascending order
    df_60entries = dfx.tail(60)

    # calculate past ndays
    pastdays = []
    for i in date_list:
        for j, k in zip(df_60entries.loc[:, date_key], df_60entries.loc[:, agg_key]):
            if j.date() == i:
                pastdays.append(k)
                break
            else:
                continue
    
        
    return sum(pastdays)


def totals(dfa, dfab, key):
    if key == 'users':
        totalUsers = dfa.copy()
        return str(totalUsers.user_id.count())
    if key == 'exercises':
        totalExercises = dfab.copy()
        return str(totalExercises.user_id.count())
    else:
        return '0'

'''SIGNUPS'''
def signups(dfa, key):
    dailySignups_df = dfa.copy()
    (dailySignups_df, date_key, subkeys) = manipulateDF.groupbyDF(
        dfx=dailySignups_df, 
        date_key='user_registered',
        freq='1D',
        subkeys=[],  # let list be empty if not required
        agg_dict={'user_id':'count'})
    
    if key == 'prevDay':
        return str(computePastDates(1, dailySignups_df, 'user_registered', 'user_id'))
    if key == 'last7':
        return str(computePastDates(7, dailySignups_df, 'user_registered', 'user_id'))
    if key == 'last30':
        return str(computePastDates(30, dailySignups_df, 'user_registered', 'user_id'))
    else:
        return '0'


'''ACTIVE USERS'''
def activeUsers(dfab, key):
    dailyActiveUsers_df = dfab.copy()
    # groupby filtered data
    (dailyActiveUsers_df, date_key, subkeys) = manipulateDF.groupbyDF(
        dfx=dailyActiveUsers_df, 
        date_key='exercise_ended',
        freq='1D',
        subkeys=['exercise_location', 'user_id'],  # let list be empty if not required
        agg_dict={'activeSgId':'count'})
    # count unique users
    dailyActiveUsers_df = manipulateDF.countUniqueUsers(
        dfx=dailyActiveUsers_df, 
        groupby_key=[date_key], 
        sort_key=date_key
    )
    if key == 'prevDay':
        return str(computePastDates(1, dailyActiveUsers_df, 'exercise_ended', 'activeSgId'))
    if key == 'last7':
        return str(computePastDates(7, dailyActiveUsers_df, 'exercise_ended', 'activeSgId'))
    if key == 'last30':
        return str(computePastDates(30, dailyActiveUsers_df, 'exercise_ended', 'activeSgId'))
    else:
        return '0'


'''EXERCISES'''
def exercises(dfab, key):
    dailyExercises_df = dfab.copy()
    (dailyExercises_df, date_key, subkeys) = manipulateDF.groupbyDF(
        dfx=dailyExercises_df, 
        date_key='exercise_ended',
        freq='1D',
        subkeys=[],  # let list be empty if not required
        agg_dict={'user_id':'count'})
        
    if key == 'prevDay':
        return str(computePastDates(1, dailyExercises_df, 'exercise_ended', 'user_id'))
    if key == 'last7':
        return str(computePastDates(7, dailyExercises_df, 'exercise_ended', 'user_id'))
    if key == 'last30':
        return str(computePastDates(30, dailyExercises_df, 'exercise_ended', 'user_id'))
    else:
        return '0'