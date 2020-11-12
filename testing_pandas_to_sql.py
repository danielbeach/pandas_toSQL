from glob import glob
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from random import randint


def gather_file_names() -> iter:
    file_names = glob("*divvy*.csv")
    return file_names


def combine_files(incoming_files: iter) -> pd.DataFrame:
    blah = pd.concat(
        [
            pd.read_csv(csv_file, index_col=None, header=0)
            for csv_file in incoming_files
        ],
        axis=0,
        ignore_index=True,
    )
    return blah


def pandas_smandas(file_uri: str) -> None:
    my_lovely_frame = pd.read_csv(file_uri)
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")
    print("starting to insert records.")
    say_what = randint(100, 200)
    my_lovely_frame.to_sql(
        con=engine,
        name=f"trip_data_{say_what}",
        if_exists="replace",
        chunksize=50000,
        method="multi",
    )


def main():

    gather_file_names()
    my_lovely_frame = combine_files(incoming_files=gather_file_names())
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")
    t1 = datetime.now()
    print("starting to insert records.")
    my_lovely_frame.to_sql(
        con=engine, name="trip_data", if_exists="replace", chunksize=100000, method="multi"
    )
    t2 = datetime.now()
    x = t2 - t1
    print(f"finished inserting records... it took {x}")


if __name__ == "__main__":
    main()
