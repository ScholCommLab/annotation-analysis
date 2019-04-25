import pandas as pd
from pathlib import Path
from tqdm import tqdm
import json
from pprint import pprint

# Load input data
data_dir = Path("../data")
groups = pd.read_csv(data_dir / "groups.csv")

responses = pd.read_csv(data_dir / "raw/api_responses.csv")

# Create dataframe for each comment
comments_cols = ["hypothesis_id", "group", "group_name", "course", "short",
                 "article_url", "comment_url", "user", "text", "created", "updated", "references"]

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
        article = r['uri']
        link_comment = r['links']['html']
        text = "".join(r['text'])
        created = pd.datetime.strptime(r['created'].split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
        updated = pd.datetime.strptime(r['updated'].split("+")[0], "%Y-%m-%dT%H:%M:%S.%f")
        if 'references' in r:
            references = r['references']
        else:
            references = None

        comment = [hypothesis_id, group, group_name, course, short,
                   article, link_comment, user, text, created, updated, references]
        comments.loc[len(comments)] = comment

    comments.index.name = "id"
    comments.to_csv("../data/comments/{}.csv".format(short))
