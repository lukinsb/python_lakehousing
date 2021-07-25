import pandas as pd
import awswrangler as wr


def transform_file(session):
    s3_client = session.client('s3')

    # Lendo dados do S3
    sales_df = wr.s3.read_csv("s3://lucas-lakehouse-demo/raw/sales_data_sample.csv", boto3_session=session,
                              encoding='unicode_escape')

    # Limpando os Dados
    fillna_dict = {'ADDRESSLINE2': "N.E.", "STATE": "N.A.", "TERRITORY": "N.A."}
    sales_df = sales_df.fillna(fillna_dict)
    sales_df['ORDERDATE'] = pd.to_datetime(sales_df['ORDERDATE'], infer_datetime_format=True)

    # Criando DIM Locale
    locale_columns = ['CITY', 'STATE', 'POSTALCODE', 'COUNTRY', 'TERRITORY']
    locale_dim = sales_df[locale_columns]
    locale_dim = locale_dim.drop_duplicates().sort_values(['POSTALCODE', 'STATE', 'CITY'])
    locale_dim = locale_dim.reset_index().drop('index', axis=1)
    locale_dim['LOCALE_ID'] = locale_dim.index
    wr.s3.to_parquet(df=locale_dim,
                     path="s3://lucas-lakehouse-demo/lakehouse/DIM_LOCALE/part001.parquet",
                     boto3_session=session,
                     dataset=True, mode="overwrite",
                     database="sales_database",
                     table="DIM_LOCALE")
    locale_dim.columns = [column.upper() for column in locale_dim.columns]
    sales_df = pd.merge(sales_df, locale_dim, how='inner', on=locale_columns).drop(locale_columns, axis=1)

    # Criando DIM Client
    client_columns = ['CUSTOMERNAME', 'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'CONTACTLASTNAME', 'CONTACTFIRSTNAME']
    client_dim = sales_df[client_columns]
    client_dim = client_dim.drop_duplicates().sort_values(['CUSTOMERNAME'])
    client_dim = client_dim.reset_index().drop('index', axis=1)
    client_dim['CLIENT_ID'] = client_dim.index
    wr.s3.to_parquet(df=client_dim,
                     path="s3://lucas-lakehouse-demo/lakehouse/DIM_CLIENT/part001.parquet",
                     boto3_session=session,
                     dataset=True, mode="overwrite",
                     database="sales_database",
                     table="DIM_CLIENT")
    client_dim.columns = [column.upper() for column in client_dim.columns]
    sales_df = pd.merge(sales_df, client_dim[['CUSTOMERNAME', 'CLIENT_ID']], how='inner', on='CUSTOMERNAME').drop(
        client_columns, axis=1)

    # Salvando FACT SALES Final
    wr.s3.to_parquet(df=sales_df,
                     path="s3://lucas-lakehouse-demo/lakehouse/FACT_SALES/part001.parquet",
                     boto3_session=session,
                     dataset=True, mode="overwrite",
                     database="sales_database",
                     table="FACT_SALES")

