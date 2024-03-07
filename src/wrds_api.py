import os

import pandas as pd
import wrds
from dotenv import load_dotenv


# create pgpass file
def create_pgpass_file():
    load_dotenv()

    username, password = os.getenv("WRDS_USERNAME"), os.getenv("WRDS_PASSWORD")
    pgpass_text = f"wrds-pgdata.wharton.upenn.edu:9737:wrds:{username}:{password}"

    filepath = os.path.expanduser("~/.pgpass")

    if os.path.exists(filepath):
        return

    with open(filepath, "w") as f:
        f.write(pgpass_text)
        os.chmod(filepath, 256)


def get_constituents():
    create_pgpass_file()

    db = wrds.Connection(wrds_username="benjaminjilma")

    sp500: pd.DataFrame = db.raw_sql(
        """
                            select a.*, b.date
                            from crsp.dsp500list as a,
                            crsp.dsf as b
                            where a.permno=b.permno
                            and b.date >= a.start and b.date<= a.ending
                            and b.date>='01/01/1990'
                            order by date;
                            """,
        date_cols=["start", "ending", "date"],
    )  # type: ignore

    sp500["permno"] = sp500["permno"].astype("int")
    sp500.sort_values("date", inplace=True)
    sp500 = sp500.drop_duplicates(["permno", "start", "ending"])[
        ["permno", "start", "ending"]
    ]
    sp500.rename(
        {"permno": "PERMNO", "start": "from", "ending": "thru"}, axis=1, inplace=True
    )
    sp500["thru"] = sp500["thru"].where(sp500["thru"] != "2022-12-30")
    sp500.to_csv("SP500_constituents.csv")

    db.close()

    return sp500
