# import dash
# from dash import html


# dash.register_page(
# 	__name__,
# 	path='/',
# 	name='Home Page: Analytic Apps',
# 	description='Welcome to my app',
# 	order=0,
# 	redirect_from=['/old-home-page', '/v2'],
# 	extra_template_stuff='yup'
# )

# layout = html.Div('Home Page: Analytic Apps')

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

import dash
from dash import html, dcc, callback, Input, Output
from dash.dependencies import Input, Output, ALL, State, MATCH, ALLSMALLER
import dash_bootstrap_components as dbc

# from app import app
import base64

import pytz, time
from datetime import datetime, tzinfo, timezone, timedelta, date

from data import data_pipeline, manipulateDF, data_pipeline2
# from figures import sgymOverview_figs
# from appdata import userSignups, sgymUsers
import config


# app = dash.Dash(__name__)

dash.register_page(
	__name__,
	path='/smartgym',
	name='SmartGym',
	description='Welcome to SmartGym',
	order=3,
	extra_template_stuff='ryup'
)

start_date='2020-06-01'
end_date='2022-11-29'
exercises_title = 'exercises from: '+ pd.to_datetime(start_date).strftime("%d-%b-%Y") +'  to  '+ pd.to_datetime(end_date).strftime("%d-%b-%Y")
exercisesyaxis_title = 'Number of Exercises'


def addPlotlyExpressFig(dfx, ctg_value, yaxis, grouping, graph_title, xaxis_title, yaxis_title, legend_title):     
    fig = px.bar(dfx, x=ctg_value, y=yaxis, color=grouping, title=graph_title)
    fig.update_traces(hovertemplate='Month: %{x} <br>No. of Exercises: %{y}')
    fig.update_layout(
            title={'font_size':13, "x": 0.05, "xanchor": "left"},
            xaxis_title=xaxis_title,
            xaxis_type='category',
            yaxis_title=yaxis_title,
            legend_title=legend_title,
            )
    if len(ctg_value) > 30:
        fig.update_layout(xaxis_tickangle=-45)
    

    return fig


def serve_layoutOnReload():
	global dfa, dfb, dfc, dfd, dfe, dff, dfg, dfab, dfac, df
	(dfa, dfb, dfc, dfd, dfe, dff, dfg, dfab, dfac) = data_pipeline2.sgymOverviewData()
	
	dailyExercises = dfab.copy()
	# filter data
	dailyExercises_df = manipulateDF.filter_data(
		dfx=dailyExercises, 
		dfx_dateCol=dailyExercises.exercise_ended,
		start_date=start_date,
		end_date=end_date
	)
	# groupby filtered data
	(df, date_key, subkeys) = manipulateDF.groupbyDF(
		dfx=dailyExercises_df, 
		date_key='exercise_ended',
		freq='1D',
		subkeys=['exercise_location', 'exercise_name'],  # let list be empty if not required
		agg_dict={'activeSgId':'count'})
	

	web_layout = html.Div([
					dbc.Container([
						dbc.Row(dbc.Col(html.Div(children=[html.Button('Add Chart', id='add-chart', n_clicks=0),]))),
						# html.Div(id='container', children=[])
						dbc.Row([
							dbc.Col([
								html.Div(id='container', children=[])
							], xs=12, sm=12, md=12, lg=9, xl=9),
						], justify='center'),
					], fluid=True) # end container
				]) # end html.div
	

	return web_layout

layout = serve_layoutOnReload


@callback(
    Output('container', 'children'),
    [Input('add-chart', 'n_clicks')],
    [State('container', 'children')]
)
def display_graphs(n_clicks, div_children):
    new_child = html.Div(
        # style={'width': '45%', 'display': 'inline-block', 'outline': 'thin lightgrey solid', 'padding': 10},
        children=[
            dcc.Graph(
                id={
                    'type': 'dynamic-graph',
                    'index': n_clicks
                },
                figure={}
            ),
            dcc.RadioItems(
                id={
                    'type': 'dynamic-choice',
                    'index': n_clicks
                },
                options=[{'label': 'Bar Chart', 'value': 'bar'},
                         {'label': 'Pie Chart', 'value': 'pie'}],
                value='bar',
            ),
            # machine name
            dcc.Dropdown(
                id={
                    'type': 'dynamic-dpn-s',
                    'index': n_clicks
                },
                options=[{'label': s, 'value': s} for s in np.sort(df['exercise_name'].unique())],
                multi=True,
                value=['leg press', 'leg extension'],
            ),
            # x-axis
            dcc.Dropdown(
                id={
                    'type': 'dynamic-dpn-ctg',
                    'index': n_clicks
                },
                options=[{'label': c, 'value': c} for c in list(df.columns)],
                value='exercise_ended',
                clearable=False
            ),
        ]
    )
    div_children.append(new_child)
    return div_children


@callback(
    Output({'type': 'dynamic-graph', 'index': MATCH}, 'figure'),
    [Input(component_id={'type': 'dynamic-dpn-s', 'index': MATCH}, component_property='value'),
     Input(component_id={'type': 'dynamic-dpn-ctg', 'index': MATCH}, component_property='value'),
     Input({'type': 'dynamic-choice', 'index': MATCH}, 'value')]
)
def update_graph(s_value, ctg_value, chart_choice):
    print(s_value)
    dff = df[df['exercise_name'].isin(s_value)]
    dff.exercise_ended = [date.strftime("%d-%b-%y") for date in dff.exercise_ended]
    print(dff)

    if chart_choice == 'bar':
        fig = addPlotlyExpressFig(
        dfx=dff,
        ctg_value=ctg_value,
        yaxis='activeSgId',
        grouping='exercise_location',
        graph_title='Daily '+exercises_title, 
        xaxis_title='Date' if ctg_value == 'exercise_ended' else str(ctg_value), 
        yaxis_title=exercisesyaxis_title,
        legend_title='Legend',
        )

        return fig
    elif chart_choice == 'pie':
        fig = px.pie(dff, names=ctg_value, values='activeSgId')
        return fig
