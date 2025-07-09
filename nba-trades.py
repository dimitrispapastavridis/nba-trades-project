from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime
import os

url = "https://stats.nba.com/js/data/playermovement/NBA_Player_Movement.json"
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com"
}

response = requests.get(url, headers=headers)

data = response.json()

df = pd.DataFrame(data['NBA_Player_Movement']['rows'])

df['TRANSACTION_DATE'] = pd.to_datetime(df['TRANSACTION_DATE'])

df['year'] = df['TRANSACTION_DATE'].dt.year
df['month'] = df['TRANSACTION_DATE'].dt.month

df['PLAYER_SLUG'] = df['PLAYER_SLUG'].apply(lambda x : ' '.join([component.capitalize() for component in x.split('-')]))

df['TEAM_SLUG'] = df['TEAM_SLUG'].apply(lambda x : x.capitalize())

df['TEAM_RECEIVING_PLAYER'] = df[ df['Transaction_Type'] == 'Trade' ]['TRANSACTION_DESCRIPTION'].apply(lambda x : x.split('received')[0].strip())

df['TEAM_RECEIVING_PLAYER'] = df['TEAM_RECEIVING_PLAYER'].fillna(df['TEAM_SLUG'])

df['TEAM_SENDING_PLAYER'] = df[ df['Transaction_Type'] == 'Trade' ]['TRANSACTION_DESCRIPTION'].apply(lambda x : x.split('from')[1].replace('.','').strip())

df['TEAM_SENDING_PLAYER'] = df['TEAM_SENDING_PLAYER'].fillna('-')

team_name_map = {
    'Hawks': 'Atlanta Hawks',
    'Celtics': 'Boston Celtics',
    'Nets': 'Brooklyn Nets',
    'Hornets': 'Charlotte Hornets',
    'Bulls': 'Chicago Bulls',
    'Cavaliers': 'Cleveland Cavaliers',
    'Mavericks': 'Dallas Mavericks',
    'Nuggets': 'Denver Nuggets',
    'Pistons': 'Detroit Pistons',
    'Warriors': 'Golden State Warriors',
    'Rockets': 'Houston Rockets',
    'Pacers': 'Indiana Pacers',
    'Clippers': 'LA Clippers',
    'Lakers': 'Los Angeles Lakers',
    'Grizzlies': 'Memphis Grizzlies',
    'Heat': 'Miami Heat',
    'Bucks': 'Milwaukee Bucks',
    'Timberwolves': 'Minnesota Timberwolves',
    'Pelicans': 'New Orleans Pelicans',
    'Knicks': 'New York Knicks',
    'Thunder': 'Oklahoma City Thunder',
    'Magic': 'Orlando Magic',
    'Sixers': 'Philadelphia 76ers',
    'Suns': 'Phoenix Suns',
    'Blazers': 'Portland Trail Blazers',
    'Kings': 'Sacramento Kings',
    'Spurs': 'San Antonio Spurs',
    'Raptors': 'Toronto Raptors',
    'Jazz': 'Utah Jazz',
    'Wizards': 'Washington Wizards'
}

df['TEAM_RECEIVING_PLAYER'] = df['TEAM_RECEIVING_PLAYER'].replace(team_name_map)

df['synced_at'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

df['synced_at'] = pd.to_datetime(df['synced_at'])

df['contract'] = df['TRANSACTION_DESCRIPTION'].apply(lambda x : x.split('to a')[1][:-1].strip() if 'to a' in x else None)

df['contract'] = df['contract'].apply(lambda x : x.replace('n ','Converted to ') if x == 'n NBA Contract' else x)

df = df[['Transaction_Type','GroupSort','PLAYER_SLUG','contract','TEAM_RECEIVING_PLAYER','TEAM_SENDING_PLAYER','TRANSACTION_DATE','year','month','synced_at']]

df.columns = [col.lower() for col in df.columns]

if not os.path.isfile('/app/csv/nba-trades.csv'):
   df.to_csv('/app/csv/nba-trades.csv', index= False)
else:
   df.to_csv('/app/csv/nba-trades.csv', mode='a', index=False, header=False)


engine = create_engine(
'postgresql+psycopg2:'
'//postgres:'    
'docker'            
'@postgresdb:5432/'      
'postgres')

con = engine.connect()

sql = """
    create table if not exists nba_trades (
    transaction_type VARCHAR(50),
    groupsort VARCHAR(50),
    player_slug VARCHAR(50),
    contract VARCHAR(50),
    team_receiving_player VARCHAR(50),
    team_sending_player VARCHAR(50),
    transaction_date DATE,
    year INT,
    month INT,
    synced_at TIMESTAMP
);
"""

with engine.connect().execution_options(autocommit=True) as conn:
    query = conn.execute(text(sql))

existing = pd.read_sql("SELECT groupsort FROM nba_trades", con)

df_new = df[~df['groupsort'].isin(existing['groupsort'])]

if len(df_new) != 0:
    df_new.to_sql("nba_trades", con=con, if_exists="append", index=False)
    print(f"Has been added {len(df_new)} new records")
else:
    print("No new entries")

#optional
print(pd.read_sql_query("""
select * from nba_trades
""", con))
