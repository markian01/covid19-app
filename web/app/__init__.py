'''
Cloud Engine App to serve data studio reports
'''
import re
import requests
from datetime import datetime

from flask import Flask, render_template
from google.cloud import bigquery
from bs4 import BeautifulSoup
from country_converter import CountryConverter
import pandas as pd

from app.helpers import Logger, BigQueryHelper

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0


@app.route('/')
def main():
    '''
    App main entrypoint
    '''
    # Get source_dataset.daily_stats' last_update
    logger = Logger('main')
    bigquery_helper = BigQueryHelper(logger)
    query = 'select max(partition_id) as date from [source_dataset.daily_stats$__PARTITIONS_SUMMARY__]'
    frame = bigquery_helper.run_query(query, _return=True, use_legacy_sql=True, use_query_cache=False)
    last_update = datetime.strptime(str(frame['date'][0]), '%Y%m%d').date()

    # Render last_update, sources to html
    sources = ['https://github.com/CSSEGISandData/COVID-19',
               'https://www.worldometers.info']
    return render_template('index.html', last_update=last_update, sources=sources)

@app.route('/update-source', methods=['POST'])
def update_source():
    '''
    Update source_dataset with new updates.
    For billing purposes, all tables updates is done through this endpoint.
    Scheduled to be run by Cloud Scheduler.
    '''
    def update_daily_stats():
        '''
        Update daily_stats using John Hopkins' data
        '''
        def format_frame(frame, name):
            '''
            Transform and rename columns
            '''
            # Transform
            frame = frame.set_index(['Country/Region', 'Province/State', 'Lat', 'Long']).stack().reset_index()
            frame = frame.rename(columns={'level_4': 'date', 0: name})
            frame['date'] = pd.to_datetime(frame['date'])
            
            # Rename columns
            frame.columns = map(lambda s: s.lower().replace('/', '_'), frame.columns)

            return frame

        BASE_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/{}'
        FILES = [('confirmed', 'time_series_covid19_confirmed_global.csv'), 
                 ('deaths', 'time_series_covid19_deaths_global.csv')]
        
        # Helper for update-source endpoint
        logger = Logger('update-source')
        bigquery_helper = BigQueryHelper(logger)

        # ETL CSV files. Ideally, this should be done incrementally (i.e. load updates on a daily basis)
        # instead of reading and loading the whole csv file every time there's an update. 
        # This workaround is done to leave all the cleaning tasks to the author(s) of the file
        # for a more consistent quality of data.
        joined_frame = pd.DataFrame()
        for name, _file in FILES:
            logger.info(f'Processing file: { _file }')
            frame = pd.read_csv(BASE_URL.format(_file))
            frame = format_frame(frame, name)
            if joined_frame.empty:
                joined_frame = frame
            else:
                joined_frame = joined_frame.merge(frame)

        # Add country_code column
        cc = CountryConverter()
        country_unique = joined_frame.country_region.unique().tolist()
        mapping = dict(zip(country_unique, cc.convert(country_unique)))
        joined_frame['country_code'] = joined_frame.country_region.map(mapping)

        # Load dataframe to BigQuery
        table_id = 'source_dataset', 'daily_stats'
        bigquery_helper.load_table(*table_id, frame=joined_frame, partition='date', 
                                clustering_fields=['country_region', 'province_state'], 
                                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    def update_world_stats():
        '''
        Update world_stats using worldometers' data
        '''
        def format_frame(frame):
            '''
            Rename and correct column values
            '''
            # Rename columns
            pattern = re.compile('[a-z]| |#').search
            cleaner = lambda col: ''.join(filter(pattern, col.lower())).strip()
            frame.columns = map(lambda col: col.replace(' ', '_'), map(cleaner, frame.columns))
            
            # Correct column values
            frame = frame.drop('#', axis=1)
            cols = frame.columns[1:] # exclude country
            cleaner = lambda row: re.sub(re.compile(',|%'), '', row).strip()
            for col in cols:
                frame[col] = frame[col].apply(cleaner)
                frame.loc[frame[col].isin(['', 'N.A.']), col] = 'nan'
                frame[col] = frame[col].astype(float)

            # Add country_code column
            cc = CountryConverter()
            frame['country_code'] = cc.convert(frame.country_or_dependency.tolist())

            return frame

        logger = Logger('world-stats')
        bigquery_helper = BigQueryHelper(logger)

        # Extract data from page
        URL = 'https://www.worldometers.info/world-population/population-by-country/'
        resp = requests.get(URL)
        soup = BeautifulSoup(resp.content, 'html.parser')

        # Parse
        rows = soup.find_all('tr')
        columns = [r.text for r in rows[0].find_all('th')]
        for i in range(1, len(rows)):
            rows[i] = [r.text for r in rows[i].find_all('td')]
        frame = pd.DataFrame(rows[1:], columns=columns)
        frame = format_frame(frame)

        # Load dataframe to BigQuery
        table_id = 'source_dataset', 'world_stats'
        bigquery_helper.load_table(*table_id, frame=frame, 
                                   create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                                   write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    # Update!
    update_daily_stats()
    update_world_stats()

    return '', 200

@app.route('/update-output', methods=['POST'])
def update_output():
    '''
    Update tables in the output_dataset. For billing purposes,
    all tables updates is done through this endpoint.
    Scheduled to be run by Cloud Scheduler. Alternatively, can also be scheduled by BigQuery,
    but the feature is not yet available in the region.
    '''
    def update_daily_stats():
        '''
        Update daily_stats from source updates
        '''
        logger = Logger('update-output')
        bigquery_helper = BigQueryHelper(logger)

        # Again, this can done incrementally if the `update_source` is also done incrementally.
        # Since we don't know what happens to the source table every update(e.g. corrective tasks, etc.), 
        # we're forced to query the whole table everytime.
        query = '''
            select
                daily_stats.*,
                if(prev_confirmed > 0, round(confirmed_growth / prev_confirmed, 2), null) as confirmed_growth_rate,
                if(prev_deaths > 0, round(deaths_growth / prev_deaths, 2), null) as deaths_growth_rate,
                # if(prev_recovered > 0, round(recovered_growth / prev_recovered, 2), null) as recovered_growth_rate,
                # if(prev_live_cases != 0, round(live_cases_growth / prev_live_cases, 2), null) as live_cases_growth_rate,
                population,
                confirmed_growth / population * 1000000 as confirmed_growth_per_million,
                deaths_growth / population * 1000000 as deaths_growth_per_million,
                # recovered_growth / population * 1000000 as recovered_growth_per_million
            from (
                select
                    *,
                    confirmed - prev_confirmed as confirmed_growth,
                    deaths - prev_deaths as deaths_growth,
                    # recovered - prev_recovered as recovered_growth,
                    # live_cases - prev_live_cases as live_cases_growth
                from (
                    select
                        *,
                        # confirmed - (deaths + recovered) as live_cases,
                        COALESCE(lag(confirmed) over(partition by country_region, province_state order by date), 0) as prev_confirmed,
                        COALESCE(lag(deaths) over(partition by country_region, province_state order by date), 0) as prev_deaths,
                        # COALESCE(lag(recovered) over(partition by country_region, province_state order by date), 0) as prev_recovered,
                        # COALESCE(lag(confirmed - (deaths + recovered)) over(partition by country_region, province_state order by date), 0) as prev_live_cases
                    from
                        `source_dataset.daily_stats`)) as daily_stats
            join
                `source_dataset.world_stats` as world_stats
            using
                (country_code)'''

        # Run query
        table_id = 'output_dataset', 'daily_stats'
        table_ref = bigquery_helper.get_table_ref(*table_id)
        bigquery_helper.run_query(query, partition='date', destination=table_ref,  
                                clustering_fields=['country_region', 'province_state'],
                                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                use_query_cache=False)

    def update_days_after_nth_case():
        '''
        Update days_after_nth_case from output_dataset.daily_stats
        '''
        logger = Logger('update-days_after_nth_case')
        bigquery_helper = BigQueryHelper(logger)

        # Same as before, this can be done incrementally.
        query = '''
            select
                *,
                row_number() over(partition by country_region_ order by date) as days_after_nth_case
            from (
                select
                    country_region as country_region_,
                    date,
                    sum(confirmed) as confirmed,
                    sum(deaths) as deaths,
                    # sum(recovered) as recovered,
                    # sum(live_cases) as live_cases,
                from
                    `output_dataset.daily_stats`
                group by
                    country_region_,
                    date
                having
                    confirmed >= 100)'''

        # Run query
        table_id = 'output_dataset', 'days_after_nth_case'
        table_ref = bigquery_helper.get_table_ref(*table_id)
        bigquery_helper.run_query(query, destination=table_ref, 
                                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                use_query_cache=False)
        
    # Update!
    update_daily_stats()
    update_days_after_nth_case()

    return '', 200
