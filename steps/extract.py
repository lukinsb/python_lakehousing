import requests



def extract_file(session):
    s3_client = session.client('s3')
    file = requests.get('https://raw.githubusercontent.com/ambientelivre/iguana/master/iserver/lib/pentaho/biserver-ce/data/csv/sales_data_sample.csv')
    response = s3_client.put_object(Body=file.content, Bucket='lucas-lakehouse-demo', Key='raw/sales_data_sample.csv')


