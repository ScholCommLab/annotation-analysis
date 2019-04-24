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
    config = configparser.ConfigParser()
    config.read('../config.cnf')

    api_key = config.get('hypothesis', 'api_key')
    api = HypothesisAPI(api_key)

    # Load input data
    data_dir = Path("../data")
    groups = pd.read_csv(data_dir / "groups.csv")

    # Query each URL/group for comments
    response_cols = ["group_name", "course", "short", "total", "resp"]
    responses = pd.DataFrame(columns=response_cols)

    iterrows = list(groups.iterrows())
    for ix, row in tqdm(iterrows, desc="Querying Hypothes.is"):
        group = row['group']
        name = row['group_name']
        course = row['course']
        short = row['short_hand']

        params = {
            'limit': 200,
            'offset': 0,
            'group': group
        }

        resp = api.search(params)
        total = resp['total']

        rows = []
        offset = 0
        rows.extend(resp['rows'])

        pbar = tqdm(total=total, desc=name, leave=False)
        while len(rows) < total:
            params['offset'] = params['offset'] + 200
            resp = api.search(params)
            rows.extend(resp['rows'])
            pbar.update(200)
        pbar.close()

        responses.loc[group] = [name, course, short, total, json.dumps(rows)]
    responses.index.name = "group"
    responses.to_csv(data_dir / "raw/api_responses.csv")

    # Create dataframe for each comment
    comments_cols = ["hypothesis_id", "group", "group_name", "course", "short",
                     "user", "text", "created", "updated", "references"]

    for ix, row in tqdm(list(responses.iterrows()), desc="Parsing annotations"):
        comments = pd.DataFrame(columns=comments_cols)
        group = ix
        course = row['course']
        group_name = row['group_name']
        total = row['total']
        short = row['short']

        rows = json.loads(row['resp'])

        for r in tqdm(rows, total=total, desc=group_name, leave=False):
            hypothesis_id = r['id']
            user = r['user'][5:-12]
            text = "".join(r['text'])
            created = pd.datetime.strptime(r['created'].split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
            updated = pd.datetime.strptime(r['updated'].split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
            if 'references' in r:
                references = r['references']
            else:
                references = None

            comment = [hypothesis_id, group, group_name, course, short,
                       user, text, created, updated, references]
            comments.loc[len(comments)] = comment

        comments.index.name = "id"
        comments.to_csv("../data/comments/{}.csv".format(short))
