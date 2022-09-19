FROM apache/airflow:2.3.3-python3.8
RUN mkdir -p dbt-athena 
RUN chmod 777 dbt-athena 
COPY dbt-athena dbt-athena
RUN pip install awswrangler \
                apache-airflow-providers-amazon \
                dbt-core \
                boto3 \
                papermill 

                
RUN pip install /opt/airflow/dbt-athena                 
RUN rm -rf opt/airflow/dbt-athena
