import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_NAME = "whsmobofbuwl6q10ily711jo9"  # User name on spotify
TOKEN = "BQA3yOX1oePqD_4wlDqzP7IXexroUS5s5ZoAOknePlGilgXEP-Cv6RSr2eBVxIhvqhOgzLcbZhyA4ka1crtVZse8kwG59AXYbm7yrRIsVvh3ykxnPETF_Yrgp1hVF-JsI2cd0hLktzEuj8PtUF9DJmweRJ3bh5cbsbtS"


""" 
This method is responsible on the validation of the data.
In case the data is empty nor there is duplicate in it, 
this method will take care of it.
"""

def check_data_validation(df: pd.DataFrame) -> bool:
    # check if data is empty
    if df.empty:
        print("No songs downloaded.")
        return False

    # check for duplicates -> played_at as primary key
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated! There are duplicates")

    # check for Nulls
    if df.isnull().values.any():
        raise Exception("Null value found!")

    # Check that all the collected data is from the last 24 hours
    ystday = datetime.datetime.now() - datetime.timedelta(days=1)
    ystday = ystday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps2 = df["timestamps"].tolist()
    for timestamp in timestamps2:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != ystday:
            raise Exception("One or more songs are not from yesterday")
    return True


if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    # making sure I'm genarting the songs within the last 24 hours each time
    today = datetime.datetime.now()
    yestrday = today - datetime.timedelta(days=1)
    yestrday_unix = int(yestrday.timestamp()) * 1000

    # request URL
    request = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yestrday_unix), headers=headers)

    data = request.json()
    # lists
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # retrieving the data from the json object into lists
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dictonary = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamps": timestamps
    }
    # building a panda frame for the retrieved data
    song_df = pd.DataFrame(song_dictonary, columns=["song_name", "artist_name", "played_at", "timestamps"])

    if check_data_validation(song_df):
        print("Data is valid, proceed to Load stage")

    # LOAD STAGE
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    cont = sqlite3.connect('my_played_tracks.sqlite')
    cursor = cont.cursor()

    # creating a table for the data or add data in case the table already exists
    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamps VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )  
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    # load data to our database
    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database!")

    cont.close()
    print("Closed database successfully")

    print(song_df)
