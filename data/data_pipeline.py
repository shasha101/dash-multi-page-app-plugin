# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from os import rename
import pandas as pd
from pandas import DataFrame
from pandas import json_normalize

import numpy as np

import pymongo
from pymongo import MongoClient

import pytz, time
from datetime import datetime, tzinfo, timezone, timedelta, date

import random
from sqlalchemy import create_engine
import argparse
import math
import base64

import config


# %%
''' GET DATA FROM MONGODB '''
def getMongoData():
    try:
        # comment out below for production
        # client = MongoClient('mongodb://192.168.8.35:27017')
        client = MongoClient('mongodb://192.168.8.29:27017')
        # client = MongoClient('mongodb://localhost:27017')

        ''' UNCOMMENT BELOW IN app.py FOR PRODUCTION DEPLOYMENT '''
        # mongo_ip = "mongodb" 	
        # mongo_port = 27017	
        # client = MongoClient(mongo_ip, mongo_port)

        # create variables to point to database client collection
        db = client['gym']
        users = db.users
        exercises = db.exercises
        bodymetrics = db.bodymetrics
        campaigns = db.campaigns
        campaignData = db.usercampaignstatus
        cumulChallenge = db.cumulative_challenges
        cumulChallengeData = db.user_cumulative_challenge_status
    
    except Exception as e:
        print("Unable to get data error: " + str(e))


    ''' DATA PIPELINE: USERS '''
    # aggregation stage(s) below:
    # print('Process data in the pipeline...')
    
    users_sortByDate = {
        '$sort' : { 'user_registered' : -1}  # oldest= 1, latest= -1. add more filter params inside {}
    }
    
    users_matchDateRange = {
            '$match': {
                'user_registered': {
                    '$gte': config.smartgym_start,  # as of date xxxx-xx-xx
                }
            }
    }

    users_projectColumns = {
        '$project' : { 
            '_id' : 1, 
            'activeSgId':1,
            'user_gender':1,
            'user_phone_no':1,
            'user_registered' : 1,
            'registered_date.location':1,
            'registered_date.machineUUID':1,
            'registered_date.time':1,
            'user_dob' : 1, 
        },#project
    }


    # pipeline list of aggregation stage(s). add [list] if have more than 1 stage
    users_pipeline = [users_matchDateRange, users_sortByDate, users_projectColumns]
    # store aggregated result in a variable
    users_agg = users.aggregate(users_pipeline, allowDiskUse=True)



    ''' DATA PIPELINE: EXERCISES '''
    # aggregation stage(s) below:
    exercises_sortByDate = {
        '$sort' : { 'exercise_ended' : -1}  # oldest= 1, latest= -1. add more filter params inside {}
    }

    exercises_matchDateRange = {
            '$match': {
                'exercise_ended': {
                    '$gte': config.smartgym_start,  # as of date xxxx-xx-xx
                }
            }
    }

    exercises_projectColumns = {
        '$project' : { 
            '_id':0,
            'user_id':1,
            'exercise_machine_id':1,
            'exercise_name':1,
            'exercise_location':1,
            'exercise_type':1,
            'exercise_started':1,
            'exercise_ended':1,
            'exercise_summary':1,
            'created':1,
        },#project
    }


    # pipeline list of aggregation stage(s). add [list] if have more than 1 stage
    exercises_pipeline = [exercises_sortByDate, exercises_matchDateRange, exercises_projectColumns]
    # store aggregated result in a variable
    exercises_agg = exercises.aggregate(exercises_pipeline, allowDiskUse=True)

    
    
    ''' DATA PIPELINE: BODYMETRICS '''

    # aggregation stage(s) below:
    bodymetrics_sortByDate = {
        '$sort' : { 'created' : -1}  # oldest= 1, latest= -1. add more filter params inside {}
    }

    bodymetrics_matchDateRange = {
            '$match': {
                'created': {
                    '$gte': config.smartgym_start,  # as of date xxxx-xx-xx
                }
            }
    }

    bodymetrics_projectColumns = {
        '$project' : { 
            '_id':0, 'user_id':1, 'machineId':1, 'exercise_location':1, 
            'created':1, 'weighing_scale_data.weight':1, 'weighing_scale_data.height':1,
            'weighing_scale_data.bmi':1 ,
        },#project
    }


    # pipeline list of aggregation stage(s). add [list] if have more than 1 stage
    bodymetrics_pipeline = [bodymetrics_sortByDate, bodymetrics_matchDateRange, bodymetrics_projectColumns]


    # store aggregated result in a variable
    bodymetrics_agg = bodymetrics.aggregate(bodymetrics_pipeline, allowDiskUse=True)
    ''' DATA PIPELINE: CAMPAIGNS '''
    campaigns_projectColumns = {
        '$project' : { 
            '_id':1, 'name':1, 'location':1, 'user_id':1, 'start_date':1, 'end_date':1 ,
        },#project
    }


    ''' DATA PIPELINE: CAMPAIGN DATA '''

    campaignData_projectColumns = {
        '$project' : { 
            '_id':0, 'user_id':1, 'campaign_id':1, 'campaign_status':1, 'user_claims':1 ,
        },#project
    }


    # pipeline list of aggregation stage(s). add [list] if have more than 1 stage
    campaignData_pipeline = [campaignData_projectColumns]


    # store aggregated result in a variable
    campaignData_agg = campaignData.aggregate(campaignData_pipeline, allowDiskUse=True)

    
    ''' DATA PIPELINE: CAMPAIGNS '''
    campaigns_projectColumns = {
        '$project' : { 
            '_id':1, 'name':1, 'location':1, 'user_id':1, 'start_date':1, 'end_date':1 ,
        },#project
    }


    # pipeline list of aggregation stage(s). add [list] if have more than 1 stage
    campaigns_pipeline = [campaigns_projectColumns]


    # store aggregated result in a variable
    campaignsList_agg = campaigns.aggregate(campaigns_pipeline, allowDiskUse=True)


    ''' DATA PIPELINE: CUMULATIVE CHALLENGES '''
    cumulChallenge_projectColumns = {
        '$project' : { 
            '_id':1, 'name':1, 'start_date':1, 'end_date':1, 'location':1, 'reward_selection_id':1,
        },#project
    }


    # pipeline list of aggregation stage(s). add [list] if have more than 1 stage
    cumulChallenge_pipeline = [cumulChallenge_projectColumns]


    # store aggregated result in a variable
    cumulChallengeList_agg = cumulChallenge.aggregate(cumulChallenge_pipeline, allowDiskUse=True)

    
    ''' DATA PIPELINE: CUMULATIVE CHALLENGES DATA '''
    cumulChallengeData_projectColumns = {
        '$project' : { 
            '_id':0, 'user_id':1, 'cumulative_challenge_id':1, 'cumulative_challenge_status.exercise_names':1, 
            'cumulative_challenge_status.target':1, 'cumulative_challenge_status.progress':1,
            'cumulative_challenge_status.user_claims':1, 'user_claims':1},#project
    }


    # pipeline list of aggregation stage(s). add [list] if have more than 1 stage
    cumulChallengeData_pipeline = [cumulChallengeData_projectColumns]


    # store aggregated result in a variable
    cumulChallengeData_agg = cumulChallengeData.aggregate(cumulChallengeData_pipeline, allowDiskUse=True)

    
    ''' DATETIME NOW '''
    localTimezone = pytz.timezone('Asia/Singapore')
    datetimeNow = datetime.now(localTimezone)

    return users_agg, exercises_agg, bodymetrics_agg, campaignData_agg, campaignsList_agg, cumulChallengeList_agg, cumulChallengeData_agg, datetimeNow       # NOTE: multiple variables returned as tuples


# %%
''' CREATE DATAFRAMES '''
def createDataframe(users_agg, exercises_agg, bodymetrics_agg, campaignData_agg, campaignsList_agg, cumulChallengeList_agg, cumulChallengeData_agg):
    # API call to store aggregated data in pandas dataframe
    dfa = json_normalize(list(users_agg))
    dfb = json_normalize(list(exercises_agg))
    dfc = json_normalize(list(bodymetrics_agg))
    dfd = pd.DataFrame(list(campaignData_agg))
    dfe = pd.DataFrame(list(campaignsList_agg))
    dff = pd.DataFrame(list(cumulChallengeList_agg))
    dfg = json_normalize(list(cumulChallengeData_agg))

    # print('Data exported to dataframe successfully!')
    
    return dfa, dfb, dfc, dfd, dfe, dff, dfg


# %%
''' HANDLE MISSING VALUES '''
def dropMissingValues(dfx, column_list):
    dfx.dropna(subset=column_list, inplace=True)

    return dfx


def fillMissingValues(dfx, values_dict):
    dfx.fillna(value=values_dict, inplace=True)

    return dfx


# %%
''' STANDARDIZE USER_ID COL AND DATA TYPE TO STRING '''
def mergeKeyDtype(dfa, dfb, dfc, dfd, dfe, dff, dfg):
    # ensure merge keys are of the same name in both dataframes
    dfa = dfa.rename(columns={"_id": "user_id"})
    dfe = dfe.rename(columns={"_id": "campaign_id", "name": "campaign_name"})
    dff = dff.rename(columns={"_id": "cumulChallenge_id", "name": "cumulChallenge_name"})
    dfg = dfg.rename(columns={"cumulative_challenge_id":'cumulChallenge_id'})

    # ensure the keys are of the exact same data type
    dfa['user_id'] = dfa['user_id'].astype('str')
    dfb['user_id'] = dfb['user_id'].astype('str')
    dfc['user_id'] = dfc['user_id'].astype('str')
    dfd['user_id'] = dfd['user_id'].astype('str')
    dfe['campaign_id'] = dfe['campaign_id'].astype('str')
    dff['cumulChallenge_id'] = dff['cumulChallenge_id'].astype('str')
    dfg['cumulChallenge_id'] = dfg['cumulChallenge_id'].astype('str')

    return dfa, dfb, dfc, dfd, dfe, dff, dfg


# %%
''' CLEANUP DATAFRAMES '''
def cleanupExerciseType(dfb):
    # rename chestpress to weight stack
    dfb['exercise_type'] = dfb['exercise_type'].replace('chestpress','weightstack')

    return dfb

def cleanupExerciseName(dfb):
    dfb['exercise_name'] =  dfb['exercise_name'].str.lower().str.replace('.*tr.*', 'Treadmill', regex=True).str.replace('.*ch.*', 'Chest Press', regex=True).str.replace('.*sh.*', 'Shoulder Press', regex=True)

    return dfb

def renameValues(dfa, dfb, dfc):
    dfa['registered_date.location'] = dfa['registered_date.location'].replace('10MBCLevel9', 'SIOT Research Lab')
    dfb['exercise_location'] = dfb['exercise_location'].replace('10MBCLevel9', 'SIOT Research Lab')
    dfc['exercise_location'] = dfc['exercise_location'].replace('10MBCLevel9', 'SIOT Research Lab')

    return dfa, dfb, dfc

def removeTestGyms(dfx):
    mask = dfx['exercise_location'].isin(['X_10MBCLevel9', 'SmartNationFiesta2019', 'ActiveSG@JLG'])
    dfx = dfx[~mask].reset_index(drop=True)

    return dfx


# %%
''' CONVERT DATETIME FROM UTC TO LOCAL '''
def convertTZ(dfx):
    try:
        df2 = dfx.select_dtypes('datetime64[ns]')
        dfx[df2.columns] = df2.apply(lambda x: x.dt.tz_localize('UTC').dt.tz_convert('Asia/Singapore'))
    except:
        pass

    return dfx


# %%
''' COMPUTE AGE BASED ON DOB '''
def getAge(dfa):
    # compute age
    today = date.today() 

    age_list = []
    for dob in dfa['user_dob']:
        if dob != 0:
            userage = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            age_list.append(userage)
        else:
            age_list.append(0)

    dfa['age'] = age_list
    dfa['age'] = dfa['age'].round()


    # print('Users age computed')

    return dfa


# %%
''' COMPUTE AGE GROUP BASED ON AGE '''
def getAgeGroup(dfa):
    # group user age into groups
    X_train_data = pd.DataFrame({'Age':dfa.age})
    bins= [0,10,13,20,30,40,50,60,100]
    labels = ['unknown','11-12','13-19','20-29','30-39','40-49','50-59','Above 60']
    dfa['ageGroup'] = pd.cut(X_train_data['Age'], bins=bins, labels=labels, right=False)


    # print('Users have beem sorted into age groups')

    return dfa


# %%
''' COMPUTE USER SIGNUPS LOCATION '''
def getSignupsLocation(dfa, dfb, dfc):
    # create new dataframe with columns of the same length
    dfx = dfa.get(['user_id', 'registered_date.location'])

    # sort dates in ascending order so that the first exercise location = signups location
    dfb = dfb.sort_values(by='exercise_ended').reset_index(drop=True)
    dfc = dfc.sort_values(by='created').reset_index(drop=True)

    location = []
    for i, j in zip(dfx.user_id, dfx['registered_date.location']):
        if (j == 'unknown'):
            try:
                x = dfb.loc[dfb['user_id'] == i, 'exercise_location'].reset_index(drop=True).loc[0]
                location.append(x)
                continue
            except:
                pass
            
            try:
                x = dfc.loc[dfc['user_id'] == i, 'exercise_location'].reset_index(drop=True).loc[0]
                location.append(x)
            except:
                location.append(j)
        else:
            location.append(j)
    
    # reassign dfa signups location column with location list
    dfa['registered_date.location'] = location


    return dfa


# %%
def mergeDF(dfx, dfy, on_key):
    # merge dataframes
    merged_df = pd.merge(dfx, dfy, how='outer', on=on_key, indicator="indicator_column")

    # only data rows with keys present in both dataframes are desired
    merged_df = merged_df[merged_df['indicator_column'] == 'both'].reset_index(drop=True)
    # merged_df_left = merged_df[merged_df['indicator_column'] == 'left_only'].reset_index(drop=True)
    # merged_df_right = merged_df[merged_df['indicator_column'] == 'right_only'].reset_index(drop=True)

    # drop indicator columns
    columns = ['indicator_column']
    merged_df.drop(columns, inplace=True, axis=1)


    return merged_df
    


# %%
''' COMPUTE CAMPAIGN SCORE '''
def getCampaignStats(dfacd):
    df_campaign = dfacd.copy()

    # print('df_campaign generated')

    # compute campaign scores for each user
    score_list = []
    start_ex_date = []
    last_ex_date = []

    for i in df_campaign['campaign_status']:
        if len(i) != 0:
            score = len(i)
            score_list.append(score)
            dateX = i[0].get('date')
            dateY = i[-1].get('date')
            start_ex_date.append(dateX)
            last_ex_date.append(dateY)
        else:
            score_list.append(0)
            start_ex_date.append(0)
            last_ex_date.append(0)

    df_campaign['campaignScore'] = score_list
    df_campaign['start_ex_date'] = start_ex_date
    df_campaign['last_ex_date'] = last_ex_date

    # remove users with score = 0
    df_campaign = df_campaign[df_campaign['campaignScore'] > 0].reset_index(drop=True)

    
    return df_campaign


# %%
''' COMPUTE CLAIM STATS '''
def getCampaignClaims(df_campaign):
    eligible_claims = []
    claims_column = []
    unclaim_column = []
    rating_column = []
    last_claim_date = []


    for i, z in zip(df_campaign['user_claims'], df_campaign['campaignScore']):
        if len(i) != 0:
            eligible = math.trunc((z-1)/4)
            eligible_claims.append(eligible)
            claim_count = len(i)
            claims_column.append(claim_count)
            unclaim = eligible - claim_count
            if unclaim < 0:
                unclaim_column.append(0)
            else:
                unclaim_column.append(unclaim)
            rating = 0
            # compute rating average per user
            for j in i:
                rating = rating + int(j.get('claim_rating'))
                date = i[-1].get('claim_date')
            rating_avg = rating/len(i)
            rating_column.append(rating_avg)
            last_claim_date.append(date)
        else:
            eligible = math.floor(z/4)
            eligible_claims.append(eligible)
            claims_column.append(0)
            unclaim_column.append(0)
            rating_column.append(0)
            last_claim_date.append(0)

    df_campaign['eligible_claims'] = eligible_claims
    df_campaign['claims_made'] = claims_column
    df_campaign['unclaim'] = unclaim_column
    df_campaign['rating_avg'] = rating_column
    df_campaign['last_claim_date'] = last_claim_date
    
    # print('df_campaign updated with claims status')
    # print('Campaign dataframe generated.')



    return df_campaign


# %%
def getData():
    # query data from mongoDB
    (users_agg, 
    exercises_agg, 
    bodymetrics_agg, 
    campaignData_agg, 
    campaignsList_agg, 
    cumulChallengeList_agg, 
    cumulChallengeData_agg, 
    datetimeNow) = getMongoData()

    # create dataframes from cursors
    (dfa, dfb, dfc, dfd, dfe, dff, dfg) = createDataframe(
        users_agg, 
        exercises_agg, 
        bodymetrics_agg, 
        campaignData_agg, 
        campaignsList_agg, 
        cumulChallengeList_agg, 
        cumulChallengeData_agg
    )


    return dfa, dfb, dfc, dfd, dfe, dff, dfg, datetimeNow


def processData():
    print('Data pipeline1 start...',  {time.strftime('%X')})
    # convert datetime to local timezone for all dataframes
    (dfa, dfb, dfc, dfd, dfe, dff, dfg, datetimeNow) = [convertTZ(x) for x in list(getData())]
    
    # process missing values for specific dataframes
    dfa = dropMissingValues(dfa, ['activeSgId'])
    dfa = fillMissingValues(
        dfx=dfa,
        values_dict={'user_dob':0, 'registered_date.location':'unknown'}
    )
    
    # set short names for collections and standardize merge key dtypes
    (dfa, dfb, dfc, dfd, dfe, dff, dfg) = mergeKeyDtype(dfa, dfb, dfc, dfd, dfe, dff, dfg)

    # more processing on dataframes
    dfab = cleanupExerciseType(dfb)
    dfb = cleanupExerciseName(dfb)
    (dfa, dfb, dfc) = renameValues(dfa, dfb, dfc)
    dfb = removeTestGyms(dfb)
    dfc = removeTestGyms(dfc)
    dfa = getAge(dfa)
    dfa = getAgeGroup(dfa)
    dfa = getSignupsLocation(dfa, dfb, dfc)

    # merge dataframes
    dfab = mergeDF(dfa, dfb, on_key='user_id')
    dfac = mergeDF(dfa, dfc, on_key='user_id')
    
    # round off bmi to 1dp
    dfac['weighing_scale_data.bmi'] = dfac['weighing_scale_data.bmi'].round(1)
    print('Data pipeline1 end...',  {time.strftime('%X')})

    return dfa, dfb, dfc, dfab, dfac, dfd, dfe, dff, dfg, datetimeNow


# %%
# page: sgym_overview
def sgymOverviewData():
    # pull latest data from server
    (dfa, dfb, dfc, dfab, dfac, dfd, dfe, dff, dfg, datetimeNow) = processData()
    gyms = list(dfab.exercise_location.unique())
    
    return dfa, dfb, dfab, dfac, gyms, datetimeNow


# %%
# page: sgym_campaign
def sgymCampaignData(dfac, dfd, dfe):
    dfacd = mergeDF(dfac, dfd, on_key='user_id')
    # retain latest BMI recorded
    dfacd = dfacd.sort_values(by = 'created').drop_duplicates('user_id',keep='last').reset_index(drop=True)
    # rename columns
    dfacd = dfacd.rename(columns={"exercise_location": "weighing_location", "created": "last_weigh-in"})


    dfacde = mergeDF(dfacd, dfe, on_key='campaign_id')
    # retain latest campaign event recorded, unique users in case users participated multiple campaigns
    dfacde = dfacde.sort_values(by=['start_date']).reset_index(drop=True)
    # re-assign dfacde into dfacd
    del dfacd
    dfacd = dfacde.copy()

    
    df_campaign = getCampaignStats(dfacd)
    df_campaign = getCampaignClaims(df_campaign)

    # add campaign period to campaign name
    camp_period = []
    for i, j in zip(df_campaign.start_date.dt.date, df_campaign.end_date.dt.date):
        camp_period.append(str(i) + ' to ' + str(j))

    campaign = []
    for i, j in zip(camp_period, df_campaign.campaign_name):
        campaign.append(i + ' - ' + j)

    df_campaign['campaign_name'] = campaign

    print('sgymCampaign dataframes created.')


    return df_campaign

def sgymCumulChallenge(dfa, dfab, dff, dfg):
    df_virtualRuns = mergeDF(dff, dfg, on_key='cumulChallenge_id')
    # add more user attributes like age group
    df_virtualRuns = mergeDF(df_virtualRuns, dfa, on_key='user_id')
    print('sgymCumulChallenge dataframes created.')


    return df_virtualRuns

def sgymEventsData():
    # pull latest data from server
    (dfa, dfb, dfc, dfab, dfac, dfd, dfe, dff, dfg, datetimeNow) = processData()
    
    df_campaign = sgymCampaignData(dfac, dfd, dfe)
    df_virtualRun = sgymCumulChallenge(dfa, dfab, dff, dfg)
    gyms = list(dfab.exercise_location.unique())

    del dfa, dfb, dfc, dfab, dfac, dfd, dfe, dff, dfg


    return df_campaign, df_virtualRun, gyms, datetimeNow


# %%
# # UNCOMMENT for testing
# import plotly.graph_objects as go
# import plotly.offline as pyo
# import plotly.figure_factory as ff
# import plotly.express as px
# from data import manipulateDF, homepage_calculator, events_calculator
# from figures import sgymOverview_figs, sgymCampaign_figs
# from appdata import userSignups, sgymUsers, exercises


# # invoke methods
# (dfa, dfb, dfab, dfac, gyms, datetimeNow) = sgymOverviewData()
# (df_campaign, df_virtualRun, gyms, datetimeNow) = sgymEventsData()


# %%
# # date mask for testing
# def filtered_data(dfx, date_column, start_date, end_date):
#     mask = (
#         (date_column.dt.date >= pd.to_datetime(start_date))     # chg to proper column names
#         & (date_column.dt.date <= pd.to_datetime(end_date))
#     )# daily sgym users

#     dfx = dfx.loc[mask, :].reset_index(drop=True)

#     return dfx, start_date, end_date

# (totalUsers, start_date, end_date) = filtered_data(dfa, dfa.user_registered, start_date='2020-06-01', end_date='2021-11-29')