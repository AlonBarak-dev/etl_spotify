import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite://my_played_tracks.sqlite"
USER_NAME = "whsmobofbuwl6q10ily711jo9"
TOKEN = "BQA_ovyAFMI2RUZ5qNYVEUa6iBWKynNovOhY89-fBaq8rFKNJekuZ03aqTNwkzpnIaks3A_rxTgrfqN7SY82iAAng8ltHCHXo6AJXBVHiYWTqnDtUjiteHA268zqCPuWtMmqmpAGCye5eNQhzCUNK-oBmhsiDj0DVg2e"

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
    request = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yestrday_unix), headers=headers)

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
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamps" : timestamps
    }
    # building a panda frame for the retrieved data
    song_df = pd.DataFrame(song_dictonary, columns= ["song_name", "artist_name", "played_at", "timestamps"])

    print(song_df)