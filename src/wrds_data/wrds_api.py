import os

from dotenv import load_dotenv

import wrds


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

    return username


def connect_wrds():
    username = create_pgpass_file()
    db = wrds.Connection(wrds_username=username)
    return db
