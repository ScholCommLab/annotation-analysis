import configparser
from pathlib import Path

import pandas as pd
import requests
import urltools
from tqdm import tqdm
import json


class HypothesisAPI(object):
    def __init__(self, api_key):
        self.api_url = "https://hypothes.is/api"
        self.search_url = self.api_url + "/search"
        self.api_key = api_key

        self.headers = {
            'Authorization': 'Bearer ' + self.api_key,
            'Content-Type': 'application/json;charset=utf-8'
        }

    def search(self, params):
        """
        Collect hypothesis data for those readings
        """
        r = requests.get(
            self.search_url,
            headers=self.headers,
            params=params)

        return r.json()


if __name__ == "__main__":
    # Init
    Config = configparser.ConfigParser()
    Config.read('../config.cnf')

    api_key = Config.get('hypothesis', 'api_key')
    api = HypothesisAPI(api_key)

    # Load input data
    data_dir = Path("../data")
    readings = pd.read_csv(data_dir / "readings.csv")

    # Query each URL/group for comments
    response_cols = ["total", "resp"]
    responses = pd.DataFrame(columns=response_cols)

    for ix, row in tqdm(list(readings.iterrows()), desc="Querying Hypothes.is"):
        rows = []
        offset = 0
        group = row['group']

        params = {
            'url': row['url'],
            'limit': 200,
            'offset': offset,
            'group': group
        }

        resp = api.search(params)
        total = resp['total']
        rows.extend(resp['rows'])
        while len(rows) < total:
            params['offset'] = params['offset'] + 200
            resp = api.search(params)
            rows.extend(resp['rows'])

        responses.loc[ix] = [total, json.dumps(rows)]
    responses.index.name = "reading_id"
    responses.to_csv(data_dir / "hypothesis_responses.csv")

    # Create dataframe for each comment
    comments_cols = ["hypothesis_id", "reading_id",
                     "user", "text", "created", "updated", "references"]
    comments = pd.DataFrame(columns=comments_cols)

    for ix, row in tqdm(list(responses.iterrows()), desc="Parsing annotations"):
        rows = json.loads(row['resp'])
        reading_id = int(ix)
        for r in rows:
            hypothesis_id = r['id']
            user = r['user'][5:-12]
            text = "".join(r['text'])
            created = pd.datetime.strptime(r['created'].split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
            updated = pd.datetime.strptime(r['updated'].split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
            if 'references' in r:
                references = r['references']
            else:
                references = None

            comment = [hypothesis_id, reading_id, user, text, created, updated, references]
            comments.loc[len(comments)] = comment

    # comments['in_class'] = comments.user.map(lambda x: x in list(usernames))
    comments.index.name = "id"
    comments.to_csv("../data/comments.csv")
