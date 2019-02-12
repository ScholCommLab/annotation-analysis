import pandas as pd
from pathlib import Path


if __name__ == "__main__":
    data_dir = Path("../data")
    input_files = list(data_dir.glob("Hypothesis*.csv"))

    # Parse input CSVs and create spreadsheets for processing
    readings_cols = ["course", "course_title", "instructor", "email", "group", "group_name",
                     "date", "title", "url"]
    readings = pd.DataFrame(columns=readings_cols)

    for f in input_files:
        # reading info
        df = pd.read_csv(f, parse_dates=["Date"])

        # course info
        course = df["Course Number"][0]
        course_title = df["Course Name"][0]
        instructor = df["First Name"][0] + " " + df["Last Name "][0]
        email = df["Email"][0]
        groups = df['Hypothesis Groups'][0].split("\n")
        group_ids = [group.split("/")[-2] for group in groups]
        group_names = [group.split("/")[-1] for group in groups]

        for ix, row in df.iterrows():
            for group, group_name in zip(group_ids, group_names):
                reading = [course, course_title, instructor, email, group, group_name,
                           row['Date'], row['Title'], row['Link ']]
                readings.loc[len(readings)] = reading

    readings.index.name = "id"
    readings.to_csv(data_dir / "readings.csv")

    groups_cols = ["group", "group_name", "course", "course_title", "instructor", "email"]
    groups = readings.groupby(groups_cols)['url'].count().reset_index().set_index("group")
    groups.rename({'url': 'url_count'})
    groups['comment_count'] = None
    groups = groups.sort_values(by="course")
    groups.to_csv(data_dir / "groups.csv")
