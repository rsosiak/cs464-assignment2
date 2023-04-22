# Extract, Transform, and Load Using Prefect

In this tutorial, we'll create an ETL pipeline in Prefect that does the following:
1. Loads data from a CSV file into a Pandas dataframe
2. Transforms the dataframe and creates additional columns
3. Writes the dataframe to a SQLite database

For a more basic overview on how to use Prefect, please see the Tutorial Overview (https://docs.prefect.io/latest/tutorials/).

## Importing Libraries

To start, we'll import the necessary libraries.

```python
import io
import requests

import pandas as pd

from prefect import flow, task
from sqlalchemy import create_engine
```

## Extracting the Data

Explanation here...

```python
@task
def extract(url):
    """
    Extracts data from CSV file at url parameter and loads data into Pandas
    dataframe.
    """
    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text), sep=',')
```

## Transforming the Data

Explanation here...

```python
def number_of_rows_per_key(df, key, column_name):
    """Adds column to dataframe that consists of aggregation by a key."""
    data = df.groupby(key)[key].agg(['count'])
    data.columns = [column_name]
    return data
  ```

```python
def clean_column(column_name):
    """Cleans column by replacing spaces with underscores."""
    return column_name.lower().replace(' ', '_')
```

```python
@task
def transform(df, *args, **kwargs):
    """Transforms the dataframe using helper functions."""
    df_new_column = number_of_rows_per_key(df, 'user ID', 'number of meals')
    df = df.join(df_new_column, on='user ID')

    df.columns = [clean_column(col) for col in df.columns]

    return df
```

## Loading the Data

Explanation here...

```python
@task
def load(df, engine, table_name):
    """
    Loads dataframe into a SQLite table. Returns the number of rows in the
    table.
    """
    df.to_sql(table_name, con=engine, if_exists='replace')

    with engine.connect() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()

    return result[0]
```

## Building a Flow from the ETL Tasks

Explanation here...

```python
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
```


```python
def main():
    print(my_flow())


if __name__ == '__main__':
    main()

```
