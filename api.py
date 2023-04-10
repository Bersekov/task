from flask import Flask, jsonify
from google.cloud import bigquery
from flasgger import Swagger, swag_from
import time

app = Flask(__name__)
swagger = Swagger(app)

@app.route('/total_rows_count')
@swag_from('swag.yml')
def get_total_rows_count():
    total_rows_count = '''
    select
      count(*) as total_rows_count
    from `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
    '''
    total_rows_count = client.query(total_rows_count).to_dataframe().iloc[0]['total_rows_count']
    return jsonify({'total_rows_count': total_rows_count})

@app.route('/last_n_count_distinct_of_persons/<int:N>')
@swag_from('swag.yml')
def get_last_N_count_distinct_of_persons(N):
    last_N_count_distinct_of_persons = f'''
    select
      procedure_dat,
      count(distinct person_id) as count_distinct_of_persons
    from `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
    group by procedure_dat
    order by procedure_dat desc
    limit {N}
    '''
    last_n_count_distinct_of_persons = client.query(last_N_count_distinct_of_persons).to_dataframe().to_dict(orient='records')
    return jsonify({'last_N_count_distinct_of_persons': last_n_count_distinct_of_persons})



client = bigquery.Client.from_service_account_json('access.json')
dataset_ref = client.get_dataset('bigquery-public-data.cms_synthetic_patient_data_omop')
table_ref = dataset_ref.table('procedure_occurrence')
table = client.get_table(table_ref)

time.sleep(300)