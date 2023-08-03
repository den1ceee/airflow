from datetime import datetime
from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    # Пусть здесь будет запись в файл. Передача файлов между тасками - в Airflow 2.0 (в следующем задании)
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top10_domains():
    top_doms = pd.read_csv(TOP_1M_DOMAINS, header=None, names=['rank', 'url'])
    top_doms['domain'] = top_doms.url.apply(lambda x: x.split('.')[-1])

    top_10_doms = top_doms \
        .groupby('domain', as_index='False') \
        .agg({'url': 'count'}) \
        .rename(columns={'url': 'count'}) \
        .sort_values('count', ascending=False) \
        .head(10)

    with open('top_10_doms.csv', 'w') as f:
        f.write(top_10_doms.to_csv(index=False, header=False))

def get_longest_domain():
    top_doms = pd.read_csv(TOP_1M_DOMAINS, header=None, names=['rank', 'url'])

    top_doms['domain_length'] = top_doms['url'].apply(len)
    longest_domain_name = top_doms[['url', 'domain_length']].sort_values('domain_length', ascending=False).reset_index().loc[0]
    longest_domain_name = longest_domain_name.drop('index')

    with open('longest_domain_name.csv', 'w') as f:
        f.write(longest_domain_name.to_csv(index=False, header=False))

def airflow_rank():
    top_doms = pd.read_csv(TOP_1M_DOMAINS, header=None, names=['rank', 'url'])

    with open('airflow_rank.csv', 'w') as f:
        if top_doms[top_doms['url'] == 'airflow.com'].empty:
            f.write('airflow.com is not found')
        else:
            airflow_rank = top_doms[top_doms['url'] == 'airflow.com']['rank']
            f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10_doms.csv', 'r') as f:
        top_10_doms = f.read()

    with open('longest_domain_name.csv', 'r') as f:
        longest_domain_name = f.read()

    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()

    date = ds

    print(f'Top 10 domains zones for date {date}')
    print(top_10_doms)

    print(f'Longest domain name for date {date}')
    print(longest_domain_name)

    print(f'Position for "airflow.com" for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'de-kapitonov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 8, 1),
}

schedule_interval = '45 11 * * *'

dag = DAG('kapitonov_test', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top10 = PythonOperator(task_id='get_top10_domains',
                    python_callable=get_top10_domains,
                    dag=dag)

t2_long_dom = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_pos = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top10, t2_long_dom, t2_pos] >> t3
