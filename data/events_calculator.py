import pandas as pd
import numpy as np
from statistics import mean


'''===== GENERAL EVENTS CALCULATOR ====='''
def show_prizeCount(dfx):
    collected = str(dfx.claims_made.sum())
    eligibleClaims= str(dfx.eligible_claims.sum())
    prizesCollected = collected + ' / ' + eligibleClaims

    return prizesCollected


def getRatings(dfx):
    cond = 1
    while cond:
        try:
            avgRating = str(dfx[dfx.rating_avg > 0].reset_index(drop=True).rating_avg.mean().round(1)) + ' / 5'
        except:
            avgRating = '0 / 5'
        cond = 0
    

    return avgRating


'''===== CAMPAIGN CALCULATOR ====='''
def reconstructCampaignDF(dfx):
    dfx = pd.DataFrame({'user_id':dfx.user_id, 'activeSgId':dfx.activeSgId,
                    'user_registered':dfx.user_registered, 
                    'ageGroup':dfx.ageGroup, 'user_gender':dfx.user_gender,
                    'last_weigh-in':dfx['last_weigh-in'],
                    'weight':dfx['weighing_scale_data.weight'],
                    'bmi':dfx['weighing_scale_data.bmi'],
                    'campaign_name':dfx.campaign_name,
                    'start_date':dfx.start_date,
                    'end_date':dfx.end_date,
                    'campaignScore':dfx.campaignScore,
                    'start_ex_date':dfx.start_ex_date,
                    'last_ex_date':dfx.last_ex_date,
                    'eligible_claims':dfx.eligible_claims,
                    'claims_made':dfx.claims_made,
                    'unclaim':dfx.unclaim,
                    'rating_avg':dfx.rating_avg,
                    'last_claim_date':dfx.last_claim_date})
    

    return dfx


def computeCampaignStatus(dfx):
    score = dfx['campaignScore']
    score_data = pd.DataFrame({'campaignScore':score})

    # dynamic days
    days = (dfx.end_date[0] - dfx.start_date[0]).days

    # len of bins must > len of labels 
    score_bins = list(range(1,days))
    position_list = list(range(1,days-2))
    position_labels = ['Weigh-in']

    for i in position_list:
        y = str(i)
        position_labels.append(y)

    dfx['campaignStatus'] = pd.cut(score_data['campaignScore'], bins=score_bins, labels=position_labels, right=False)

    # convert date columns to dates only
    dfx.start_ex_date = pd.to_datetime(dfx.start_ex_date).dt.date
    dfx.last_ex_date = pd.to_datetime(dfx.last_ex_date).dt.date

    dfx = dfx[dfx.campaignStatus != 'Weigh-in'].sort_values(by='campaignStatus').reset_index(drop=True)

    return dfx


'''===== MARATHON CALCULATOR ====='''
def computeMilestone(dfx):
    # group user age into groups
    X_train_data = pd.DataFrame({'distance':dfx.distanceKM})

    total_dist = dfx.target[0]
    milestones = 10
    count = total_dist/milestones
    bins = [0]
    binx = 0

    for i in list(range(milestones+1)):
        binx+=count
        bins.append(binx)
    # extend the limit of the max(last) value
    bins[-1] = 1000
    
    labels = ['under 10%','~10%', '~20%','~30%','~40%','~50%', '~60%', '~70%', '~80%', '~90%', '100%']
    dfx['milestone'] = pd.cut(X_train_data['distance'], bins=bins, labels=labels, right=False)


    return dfx


def getMarathonClaims(dfx):
    eligible_claims = []
    claims_column = []
    unclaim_column = []
    rating_column = []
    last_claim_date = []
    reward_column = []


    for i,j,k in zip(dfx.user_claims, dfx.target, dfx.distanceKM):
        if (k >= j):   # means user completed the marathon
            eligible = 1
            eligible_claims.append(eligible)
            claim_count = len(i)
            claims_column.append(claim_count)
            unclaim = eligible - claim_count
            if unclaim <= 0:
                unclaim_column.append(0)
            else:
                unclaim_column.append(unclaim)
            
            try:
                last_claim_date.append(i[0].get('claim_date'))
                rating_column.append(i[0].get('claim_rating'))
                reward_column.append(i[0].get('reward_id'))
            except:
                last_claim_date.append(0)
                rating_column.append(0)
                reward_column.append(0)
        else:
            eligible_claims.append(0)
            claims_column.append(0)
            unclaim_column.append(0)
            rating_column.append(0)
            last_claim_date.append(0)


    dfx['eligible_claims'] = eligible_claims
    dfx['claims_made'] = claims_column
    dfx['unclaim'] = unclaim_column
    dfx['rating_avg'] = rating_column
    dfx['last_claim_date'] = last_claim_date


    return dfx