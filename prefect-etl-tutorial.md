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

First, we'll extract the data from a CSV file by making a GET request to an API. Using the Pandas library, we read the CSV and load it into a Pandas dataframe.

In this case, we are extracting data from a CSV file, but this code can be modified to extract a JSON object as well.

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

Now, we'll perform some basic transformations on the dataframe we created in the previous step. 

We will transform the data in two ways:
1. Add a column that counts the number of meals for each user.
2. Clean the column names so we can better store them in a database.

```python
def number_of_rows_per_key(df, key, column_name):
    """Adds column to dataframe that aggregates the data on some key."""
    data = df.groupby(key)[key].agg(['count'])
    data.columns = [column_name]
    return data
  ```

```python
def clean_column(column_name):
    """Cleans column by replacing spaces with underscores."""
    return column_name.lower().replace(' ', '_')
```

Putting it all together, we pass our dataframe as a parameter to the transform task, which performs the transformations we discussed above. The function returns the dataframe with the applied transformations.

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

Finally, after we have extracted and transformed our data, the last step is to load it into our database. For the purposes of this tutorial, we are loading into a simple SQLite database. However, this methodology can be extended to be used with any database (PostgreSQL, MySQL, etc.).

We load the data, and run a query to get the count of rows in our table, ensuring that the data was loaded.

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

Putting the tasks we created all together, we build our flow. You can see that the flow calls each task we built, extracting the data, transforming it, and loading it into our database. We perform some very simple validation, checking that the number of rows in our dataframe matches the number of rows inserted into our database.

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

And finally, we run the flow here.

```python
def main():
    print(my_flow())


if __name__ == '__main__':
    main()

```

And that's it! Hope this was helpful!
