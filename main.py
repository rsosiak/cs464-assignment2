import io
import requests

import pandas as pd

from prefect import flow, task


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
def load():
    return


@flow
def my_func():
    URL = (
        'https://raw.githubusercontent.com/mage-ai/datasets/master/' +
        'restaurant_user_transactions.csv'
    )
    data = extract(URL)
    data = transform(data)
    return data.head()


print(my_func())
