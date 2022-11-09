from airflow.decorators import dag, task
import datetime
import pendulum
import os
import time
import logging

import finnhub

from connect_to_mongo import connect_to_mongo_db
from constants import get_tracked_companies_list, get_finnhub_api_key

default_task_args = {
    'retries': 100,
    'retry_delay': datetime.timedelta(minutes=1)
    }

@dag(
    dag_id="realtime-social-sentiments",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2021, 11, 8, tz="UTC"),
    catchup=True,
    dagrun_timeout=datetime.timedelta(hours=12),
    max_active_runs=1,
    default_args = default_task_args
)
def RealtimeSocialSentimentsDAG():

    def convert_atTime_to_timestamp(dict_):
        dict_['seconds_timestamp'] = int(pendulum.from_format(dict_['atTime'], 'YYYY-MM-DD HH:mm:SS').timestamp())
        del dict_['atTime']
    
    def find_dict_by_time(dicts, time_key):
        for dict_ in dicts:
            if dict_['atTime'] == time_key:
                return dict_
        return None
    
    def get_weighted_from_twitter_and_reddit_sentiment(twitter, reddit):
        weighted_dict = dict()
        try:
            if twitter is None:
                weighted_dict = reddit.copy()
                weighted_dict['twitter_reddit_ratio'] = 0
            elif reddit is None:
                weighted_dict = twitter.copy()
                weighted_dict['twitter_reddit_ratio'] = 1
            else: # computes weighted_values
                weighted_dict['atTime'] = twitter['atTime'] # time from any dict would work
                total_mentions = twitter['mention'] + reddit['mention']
                twitter_reddit_ratio = twitter['mention'] / total_mentions
                reddit_twitter_ratio = 1 - twitter_reddit_ratio
                weighted_dict['twitter_reddit_ratio'] = twitter_reddit_ratio
                weighted_dict['mention'] = total_mentions
                weighted_dict['positiveScore'] = twitter['positiveScore'] * twitter_reddit_ratio + reddit['positiveScore'] * reddit_twitter_ratio
                weighted_dict['negativeScore'] = twitter['negativeScore'] * twitter_reddit_ratio + reddit['negativeScore'] * reddit_twitter_ratio
                weighted_dict['negativeMention'] = twitter['negativeMention'] + reddit['negativeMention']
                weighted_dict['positiveMention'] = twitter['positiveMention'] + reddit['positiveMention']
        except Exception as e:
            logging.error(f'Twitter dict: {twitter}, reddit dict: {reddit}')
            raise
        return weighted_dict

    @task(task_id="get_finnhub_social_sentiments")
    def get_finnhub_social_sentiments(ds=None, **kwargs):
        # date passed is 1 day ago at 00:00:00
        # data will be fetched from this period:
        # 2 days ago at 00:00:00 || <data in here will be fetched> || 2 days ago at 23:00:00
        date_to_get_to = pendulum.from_format(str(ds), fmt='YYYY-MM-DD')
        date_to_get_from = date_to_get_to.subtract(days=1)
        date_to_get_to = date_to_get_from.add(hours=23)

        companies_to_track = get_tracked_companies_list()
        api_key = get_finnhub_api_key()
        finnhub_client = finnhub.Client(api_key=api_key)
        db = connect_to_mongo_db()

        final_records = []
        for company in companies_to_track:
            logging.info(f'Fetching company f{company}')
            try:
                results = finnhub_client.stock_social_sentiment(
                    company,
                    date_to_get_from.strftime('%Y-%m-%d %H:%M:%S'),
                    date_to_get_to.strftime('%Y-%m-%d %H:%M:%S'))

                # checking if response structure is as expected
                assert 'reddit' in results, f'Results keys are {list(results.keys())}'
                assert 'symbol' in results, f'Results keys are {list(results.keys())}'
                assert 'twitter' in results, f'Results keys are {list(results.keys())}'
                assert results['symbol'] == company, f"Company is {results['symbol']} and not {company}"

                # get weighted average of twitter and reddit at same timestamp
                unique_times_reddit = set([record['atTime'] for record in results['reddit']])
                unique_times_twitter = set([record['atTime'] for record in results['twitter']])
                unique_times = list(unique_times_reddit.union(unique_times_twitter))
                for record_time in unique_times:
                    reddit_record = find_dict_by_time(results['reddit'], record_time)
                    twitter_record = find_dict_by_time(results['twitter'], record_time)
                    weighted_record = get_weighted_from_twitter_and_reddit_sentiment(twitter_record, reddit_record)
                    convert_atTime_to_timestamp(weighted_record)
                    weighted_record['company'] = company
                    final_records.append(weighted_record)

                time.sleep(1)
            except Exception as e:
                logging.error(f'Failed getting company {company}')
                raise
        ids = db['realtime-social-sentiments-finnhub'].insert_many(final_records).inserted_ids
        return f'Saved {len(ids)} sentiments'

    get_finnhub_social_sentiments()
dag = RealtimeSocialSentimentsDAG()
