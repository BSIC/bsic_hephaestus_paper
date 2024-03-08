import pandas as pd
from wrds_api import connect_wrds


def get_sp500_constituents():
    db = connect_wrds()

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
