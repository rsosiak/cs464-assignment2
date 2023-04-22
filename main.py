import io
import requests

import pandas as pd

from prefect import flow, task
from sqlalchemy import create_engine


@task
def extract(url):
    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text), sep=',')


def number_of_rows_per_key(df, key, column_name):
    data = df.groupby(key)[key].agg(['count'])
    data.columns = [column_name]
    return data


def clean_column(column_name):
    return column_name.lower().replace(' ', '_')


@task
def transform(df, *args, **kwargs):
    df_new_column = number_of_rows_per_key(df, 'user ID', 'number of meals')
    df = df.join(df_new_column, on='user ID')

    df.columns = [clean_column(col) for col in df.columns]

    return df


@task
def load(df, engine, table_name):
    df.to_sql(table_name, con=engine, if_exists='replace')

    with engine.connect() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()

    return result[0]


@flow
def my_flow():
    URL = (
        'https://raw.githubusercontent.com/mage-ai/datasets/master/' +
        'restaurant_user_transactions.csv'
    )
    TABLE_NAME = 'tutorial'
    data = extract(URL)
    data = transform(data)

    engine = create_engine('sqlite://', echo=False)
    num_rows = load(data, engine, TABLE_NAME)

    return len(data.index) == num_rows


def main():
    print(my_flow())


if __name__ == '__main__':
    main()
