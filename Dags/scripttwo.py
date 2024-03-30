# %%
#!/usr/bin/env python
import pandas as pd
import requests
import json
import psycopg2
import psycopg2.extras as extras

# %%


# %%d
# %%
def my_file():
    df = pd.read_csv(r"C:\Users\ADETUNJI\etl_with_airflow\test.csv")

    # %%
    df= pd.DataFrame(df)
    df.dtypes

    # %%
    df['pickup_datetime']=pd.to_datetime(df['pickup_datetime'])

    # %%
    df.dtypes

    # %%
    try:
      connection = psycopg2.connect(
          host="localhost",
          database="ny_taxi",
          user="root",
          password="root"
      )
      print("Connection successful!")
    except Exception as e:
      print("Connection failed:", e)
    finally:
      if connection:
        connection.close()


    # %%
    from sqlalchemy import create_engine

    # %%
    engine = create_engine('postgresql://root:root@localhost:5432/new')

    # %%


    # %%


    # %%
    print(pd.io.sql.get_schema(df,name ='test_data'))

    # %%
    df.to_sql(name='yellow',con=engine)


