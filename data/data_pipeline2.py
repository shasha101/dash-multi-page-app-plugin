# %%
import asyncio
import pandas as pd
from pandas import json_normalize

import numpy as np

from pymongo import MongoClient

import time
from datetime import date
import config

import math

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
            # 'exercise_machine_id':1,
            'exercise_name':1,
            'exercise_location':1,
            'exercise_type':1,
            # 'exercise_started':1,
            'exercise_ended':1,
            # 'exercise_summary':1,
            # 'created':1,
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
    
    return users_agg, exercises_agg, bodymetrics_agg, campaignData_agg, campaignsList_agg, cumulChallengeList_agg, cumulChallengeData_agg     # NOTE: multiple variables returned as tuples




# %%
''' CREATE DATAFRAMES '''
def createDataframe(collection):
    dfx = json_normalize(list(collection))

    
    return dfx




# %%
''' CONVERT DATETIME FROM UTC TO LOCAL '''
def convertTZ(dfx):
    try:
        dfz = dfx.select_dtypes('datetime64[ns]')
        dfx[dfz.columns] = dfz.apply(lambda x: x.dt.tz_localize('UTC').dt.tz_convert('Asia/Singapore'))
    except:
        pass

    return dfx




# %%
''' HANDLE MISSING VALUES '''
def dropMissingValues(dfx, column_list):
    dfx = dfx.dropna(subset=column_list)

    return dfx


def fillMissingValues(dfx, values_dict):
    dfx = dfx.fillna(value=values_dict)

    return dfx




# %%
''' STANDARDIZE USER_ID COL AND DATA TYPE TO STRING '''
def setDataType(dfx, dtype_dict):
    # e.g. df.astype({'col1': 'int32'}).dtypes
    dfx = dfx.astype(dtype_dict)

    
    return dfx




# %%
''' CLEANUP DATAFRAMES '''
def replaceValues(dfx, dfx_col, old_value='', new_value='', exercise_name=False):
    # if arguments not passed, default values will be taken
    # eg.(1): df.replace([0, 1, 2, 3], [4, 3, 2, 1]), positional - replace all 0's with 4 etc
    # eg.(2): df['column_name'] = df['column_name'].replace('a', 'b') - replace all a's in the column with 'b'
    
    dfx_col = dfx_col.str.lower().replace(old_value, new_value, exercise_name)   # for string data types
    if exercise_name == True:
        dfx['exercise_name'] = dfx['exercise_name'].str.lower().str.replace('.*tr.*', 'treadmill', regex=True).str.replace('.*ch.*', 'chest press', regex=True).str.replace('.*sh.*', 'shoulder press', regex=True)
    
    return dfx


def removeValues(dfx, value_list):
    mask = dfx['exercise_location'].isin(value_list)
    dfx = dfx[~mask].reset_index(drop=True)


    return dfx




# %%
''' COMPUTE AGE BASED ON DOB '''
def getAge(dfx, dfx_col):
    # compute age
    today = date.today() 

    age_list = []
    for dob in dfx_col:
        if dob != 0:
            userage = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            age_list.append(userage)
        else:
            age_list.append(0)

    dfx['age'] = age_list
    dfx['age'] = dfx['age'].round()


    return dfx




# %%
''' COMPUTE AGE GROUP BASED ON AGE '''
def getAgeGroup(dfx, dfx_col):
    # group user age into groups
    X_train_data = pd.DataFrame({'Age':dfx_col})
    bins= [0,10,13,20,30,40,50,60,100]
    labels = ['unknown','11-12','13-19','20-29','30-39','40-49','50-59','Above 60']
    dfx['ageGroup'] = pd.cut(X_train_data['Age'], bins=bins, labels=labels, right=False)


    return dfx




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
''' MERGE DATAFRAMES '''
async def mergeDF(dfx, dfy, on_key):
    # merge dataframes
    merged_df = pd.merge(dfx, dfy, how='left', on=on_key)

    return merged_df

# %%
async def processDFA(dfa, dfb, dfc):
    dfa = setDataType(dfa, {'user_id':'str'})
    dfa = getAge(dfa, dfa.user_dob)
    dfa = getAgeGroup(dfa, dfa.age)
    dfa = getSignupsLocation(dfa, dfb, dfc)

    return dfa

async def processDFB(dfb):
    dfb = setDataType(dfb, {'user_id':'str'})
    dfb = replaceValues(dfb, dfb.exercise_type, 'chestpress', 'weightstack')
    dfb = replaceValues(dfb, dfb.exercise_name, exercise_name=True)
    dfb = replaceValues(dfb, dfb.exercise_location, '10MBCLevel9', 'SIOT Research Lab')
    dfb = removeValues(dfb, ['X_10MBCLevel9', 'SmartNationFiesta2019', 'ActiveSG@JLG'])

    return dfb

async def processDFC(dfc):
    dfc = setDataType(dfc, {'user_id':'str'})
    dfc = replaceValues(dfc, dfc.exercise_location, '10MBCLevel9', 'SIOT Research Lab')
    dfc = removeValues(dfc, ['X_10MBCLevel9', 'SmartNationFiesta2019', 'ActiveSG@JLG'])
    
    return dfc

async def processDFD(dfd):
    dfd = setDataType(dfd, {'user_id':'str'})

    return dfd

async def processDFE(dfe):
    dfe = dfe.rename(columns={"_id": "campaign_id", "name": "campaign_name"})
    dfe = setDataType(dfe, {'campaign_id':'str'})

    return dfe

async def processDFF(dff):
    dff = dff.rename(columns={"_id": "cumulChallenge_id", "name": "cumulChallenge_name"})
    dff = setDataType(dff, {'cumulChallenge_id':'str'})

    return dff

async def processDFG(dfg):
    dfg = dfg.rename(columns={"cumulative_challenge_id":'cumulChallenge_id'})
    dfg = setDataType(dfg, {'cumulChallenge_id':'str'})

    return dfg

# %%
async def getData():
    print('Start fetching data from mongoDB', {time.strftime('%X')})
    (dfa, dfb, dfc, dfd, dfe, dff, dfg) = [convertTZ(y) for y in [createDataframe(x) for x in list(getMongoData())]]
    
    rawData = {'dfa':dfa, 'dfb':dfb, 'dfc':dfc, 'dfd':dfd, 'dfe':dfe, 'dff':dff, 'dfg':dfg}

    print('Done fetching and output into dataframes.',  {time.strftime('%X')})

    return rawData

async def cleanData(dfa):   # do tgt with BCDEFG
    dfa = dropMissingValues(dfa, ['activeSgId'])
    dfa = fillMissingValues(dfa, {'user_dob':0, 'registered_date.location':'unknown'})
    dfa = dfa.rename(columns={"_id": "user_id"})

    return dfa


async def main():
    # synchronous
    rawData = await getData()

    dfa = rawData.get('dfa')
    dfb = rawData.get('dfb')
    dfc = rawData.get('dfc')
    dfd = rawData.get('dfd')
    dfe = rawData.get('dfe')
    dff = rawData.get('dff')
    dfg = rawData.get('dfg')
    # synchronous
    dfa = await cleanData(dfa)

    # run all tasks below asynchronously
    dfa = await asyncio.create_task(processDFA(dfa, dfb, dfc))
    dfb = await asyncio.create_task(processDFB(dfb))
    dfc = await asyncio.create_task(processDFC(dfc))
    dfd = await asyncio.create_task(processDFD(dfd))
    dfe = await asyncio.create_task(processDFE(dfe))
    dff = await asyncio.create_task(processDFF(dff))
    dfg = await asyncio.create_task(processDFG(dfg))
    print('All DFs processed.',  {time.strftime('%X')})
    
    return dfa, dfb, dfc, dfd, dfe, dff, dfg


async def mergeDataframes(dfa, dfb, dfc):
    # merge dataframes
    print('Start merging DFs.',  {time.strftime('%X')})
    dfab = await asyncio.create_task(mergeDF(dfb, dfa, 'user_id'))
    dfac = await asyncio.create_task(mergeDF(dfc, dfa, 'user_id'))
    dfab = dropMissingValues(dfab, ['activeSgId'])
    dfac = dropMissingValues(dfac, ['activeSgId'])
    print('Merging done.',  {time.strftime('%X')})

    return dfab, dfac

# %%
''' COMPUTE CAMPAIGN SCORE '''
def getCampaignStats(dfacd):
    df_campaign = dfacd.copy()

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
    
    
    return df_campaign


# %%
# page: sgym_campaign
async def sgymCampaign(dfac, dfd, dfe):
    # only participants in campaigns
    dfacd = pd.merge(dfd, dfac[['activeSgId', 'user_registered', 'user_id','ageGroup', 
                                    'user_gender', 'created', 'weighing_scale_data.weight', 'weighing_scale_data.bmi']], on='user_id', how='left')
    # retain latest BMI recorded
    dfacd = dfacd.sort_values(by = 'created').drop_duplicates('user_id',keep='last').reset_index(drop=True)
    # rename columns
    dfacd = dfacd.rename(columns={"exercise_location": "weighing_location", "created": "last_weigh-in"})


    dfacde =  pd.merge(dfacd, dfe, on='campaign_id',how='left')
    # retain latest campaign event recorded, unique users in case users participated multiple campaigns
    dfacde = dfacde.sort_values(by=['start_date']).reset_index(drop=True)
    # re-assign dfacde into dfacd
    del dfacd
    dfacd = dfacde.copy()

    
    df_campaign = getCampaignStats(dfacd)
    df_campaign = getCampaignClaims(df_campaign)

    # drop users with no campaing_name
    df_campaign = df_campaign.dropna(subset=['campaign_name'])

    # add campaign period to campaign name
    camp_period = []
    for i, j in zip(df_campaign.start_date.dt.date, df_campaign.end_date.dt.date):
        camp_period.append(str(i) + ' to ' + str(j))

    campaign = []
    for i, j in zip(camp_period, df_campaign.campaign_name):
        campaign.append(i + ' - ' + j)

    df_campaign['campaign_name'] = campaign

    print('sgymCampaign dataframe created.')


    return df_campaign

async def sgymCumulChallenge(dfa, dfab, dff, dfg):
    df_virtualRuns =  pd.merge(dfg, dff, on='cumulChallenge_id', how='left')
    # add more user attributes like age group
    df_virtualRuns = pd.merge(df_virtualRuns, dfa[['activeSgId', 'user_id','ageGroup', 'user_gender']], on='user_id', how='left')
    df_virtualRuns = df_virtualRuns.rename(columns={'cumulative_challenge_status.target': 'target', 'cumulative_challenge_status.progress':'distanceKM'})

    df_virtualRuns.target = round(df_virtualRuns.target/1000, 1)
    df_virtualRuns.distanceKM = round(df_virtualRuns.distanceKM/1000, 1)
    # remove non-progressing users
    df_virtualRuns = df_virtualRuns[df_virtualRuns.distanceKM != 0].reset_index(drop=True)
    # compute progress status
    df_virtualRuns['progress'] = (df_virtualRuns.distanceKM/df_virtualRuns.target*100).round()
    print('sgymCumulChallenge dataframe created.')


    return df_virtualRuns

# %%
def sgymOverviewData():
    dfa, dfb, dfc, dfd, dfe, dff, dfg = asyncio.run(main())
    (dfab, dfac) = asyncio.run(mergeDataframes(dfa, dfb, dfc))

    return dfa, dfb, dfc, dfd, dfe, dff, dfg, dfab, dfac

def sgymEventsData():
    dfa, dfb, dfc, dfd, dfe, dff, dfg = asyncio.run(main())
    (dfab, dfac) = asyncio.run(mergeDataframes(dfa, dfb, dfc))
    df_campaign = asyncio.run(sgymCampaign(dfac, dfd, dfe))
    df_virtualRun = asyncio.run(sgymCumulChallenge(dfa, dfab, dff, dfg))

    return df_campaign, df_virtualRun

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
# (dfa, dfb, dfc, dfd, dfe, dff, dfg) = await main()
# (dfab, dfac) = await mergeDataframes(dfa, dfb, dfc)
# df_campaign = await sgymCampaign(dfac, dfd, dfe)
# df_virtualRun = await sgymCumulChallenge(dfa, dfab, dff, dfg)
# print('End process.',  {time.strftime('%X')})

# %%
# date mask for testing
# start_date='2020-06-01'
# end_date='2021-11-29'



