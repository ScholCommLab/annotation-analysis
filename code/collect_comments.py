import configparser
from pathlib import Path

import pandas as pd
import requests
import urltools
from hypothesisapi import *
from tqdm import tqdm


def query_altmetric(url, start):
    """
    Collect hypothesis data for those readings
    """
    params = {'url': url, 'limit': 200, 'offset': start}

    r = requests.get(API_URL + "/search", headers=headers, params=params)
    return r.json()


if __name__ == "__main__":
    # Load config
    Config = configparser.ConfigParser()
    Config.read('../config.cnf')

    api_key = Config.get('hypothesis', 'api_key')
    API_URL = "https://hypothes.is/api"
    headers = {'Authorization': 'Bearer ' + api_key,
               'Content-Type': 'application/json;charset=utf-8'}

    data_dir = Path("../data")
    input_files = list(data_dir.glob("Hypothesis*.csv"))

    # Initialise output dataframe for individual readings
    readings_cols = ["course", "instructor", "date",
                     "month", "week", "title", "url", "total", "resp"]
    readings = pd.DataFrame(columns=readings_cols)

    dfs = []
    for f in input_files:
        try:
            df = pd.read_csv(f, parse_dates=["Date"])
            df['month'] = df.Date.map(lambda x: x.month)
            df['week'] = df.Date.map(lambda x: x.week)
            df['week'] = df['week'] - min(df['week']) + 1
        except Exception as e:
            print("Skipping {}".format(f.name))
            continue

        dfs.append(df)

        course = df["Course Number"][0]
        instructor = df["First Name"][0] + " " + df["Last Name "][0]

        for ix, row in df.iterrows():
            readings.loc[len(readings)] = [course, instructor, row['Date'], row['month'],
                                           row['week'], row['Title'], row['Link '], None, None]

    df = pd.concat(dfs, sort=False)

    classes_cols = ["Course Number", "Course Name", "First Name", "Last Name ", "Email"]
    classes = df.groupby(classes_cols)["Link "].count().reset_index()
    classes.to_csv("../data/courses.csv")

    responses = []
    for ix, row in tqdm(list(readings.iterrows()), desc="Querying Hypothes.is"):
        rows = []
        offset = 0
        resp = query_altmetric(row['url'], offset)
        total = resp['total']
        rows.extend(resp['rows'])
        while len(rows) < total:
            offset = offset + 200
            resp = query_altmetric(row['url'], offset)
            rows.extend(resp['rows'])
        readings.loc[ix, 'resp'] = json.dumps(rows)
        readings.loc[ix, 'total'] = total

    readings.index.name = "id"
    readings.to_csv("../data/readings.csv")

    # Create dataframe for each comment
    comments_cols = ["url_id", "user", "text", "created", "updated", "references"]
    comments = pd.DataFrame(columns=comments_cols)

    for ix, row in tqdm(list(readings.iterrows()), desc="Parsing annotations"):
        rows = json.loads(row['resp'])
        url_id = int(ix)
        for r in rows:
            id = r['id']
            user = r['user'][5:-12]
            text = "".join(r['text'])
            created = pd.datetime.strptime(r['created'].split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
            updated = pd.datetime.strptime(r['updated'].split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
            if 'references' in r:
                references = r['references']
            else:
                references = None

            comments.loc[id] = [url_id, user, text, created, updated, references]

    # comments['in_class'] = comments.user.map(lambda x: x in list(usernames))
    comments.index.name = "id"
    comments.to_csv("../data/comments.csv")
