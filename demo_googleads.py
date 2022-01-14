import sys

import pandas as pd
import quick_utils as qu    # Helper module for communicating with SQL Server & postgreSQL
import datetime
from datetime import timedelta
from tqdm.auto import tqdm

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from google.protobuf.json_format import MessageToJson, MessageToDict



# Settings
config = configparser.ConfigParser()
config.read('config.ini')

host = config['mysql']['host']
user = config['mysql']['user']
db = config['mysql']['db']
vault = config['os']['vault']   # Uses gnome-keyring to fetch database password

qu_kwargs = {
    'vault': vault,
    'db_host': host,
    'db_name': db,
    'db_user': user,
    'db_sys' : 'SQL Server'
}
OUT_SUFFIX = ''
if len(sys.argv) > 1:
    OUT_SUFFIX = sys.argv[1]

# Authenticate and connect
config = qu.connect(**qu_kwargs)
config['log_table'] = '[google].[T_newAPI_Log]'

CUSTOMER_ID = '4433856492'
LEVELS = ['ads', 'campaigns', 'clicks']
ATTR_WINDOW = 30

def main(client, customer_id, date, load_type):
    ga_service = client.get_service("GoogleAdsService")

    if load_type == 'ads':
        table = '[google].[T_Ads_newAPI_STG%s]' % OUT_SUFFIX
        query = f"""
                SELECT ad_group_ad.ad.id,
                    ad_group.id,
                    campaign.id,
                    segments.date,
                    ad_group_ad.status,
                    ad_group_ad.ad.type,
                    ad_group_ad.ad.name,
                    ad_group_ad.ad.call_ad.headline1,
                    ad_group_ad.ad.call_ad.headline2,
                    ad_group_ad.ad.expanded_text_ad.headline_part1,
                    ad_group_ad.ad.expanded_text_ad.headline_part2,
                    ad_group_ad.ad.expanded_text_ad.headline_part3,
                    ad_group_ad.ad.legacy_responsive_display_ad.long_headline,
                    ad_group_ad.ad.legacy_responsive_display_ad.short_headline,
                    ad_group_ad.ad.text_ad.headline,
                    ad_group_ad.ad.text_ad.description1,
                    ad_group_ad.ad.text_ad.description2,
                    ad_group_ad.ad.expanded_text_ad.description2,
                    ad_group_ad.ad.expanded_text_ad.description,
                    ad_group_ad.ad.final_urls,
                    customer.currency_code,
                    metrics.impressions,
                    metrics.cost_micros,
                    metrics.cost_per_all_conversions,
                    metrics.cost_per_conversion,
                    metrics.clicks,
                    metrics.gmail_secondary_clicks,
                    metrics.conversions,
                    metrics.conversions_from_interactions_rate,
                    metrics.all_conversions_from_interactions_rate,
                    metrics.engagement_rate,
                    metrics.interactions,
                    metrics.interaction_rate,
                    metrics.percent_new_visitors,
                    metrics.top_impression_percentage,
                    metrics.value_per_all_conversions,
                    metrics.value_per_conversion
                FROM ad_group_ad
                WHERE segments.date BETWEEN '{date}' AND '{date}'
            """
    elif load_type == 'campaigns':
        table = '[google].[T_Campaigns_newAPI_STG%s]' % OUT_SUFFIX
        query = f"""
            SELECT campaign.id,
                campaign.name,
                campaign.status,
                campaign.advertising_channel_type,
                campaign.start_date,
                campaign.end_date,
                customer.id,
                customer.descriptive_name,
                customer.currency_code,
                customer.time_zone,
                metrics.absolute_top_impression_percentage,
                metrics.active_view_cpm,
                metrics.active_view_ctr,
                metrics.active_view_impressions,
                metrics.active_view_measurability,
                metrics.active_view_measurable_cost_micros,
                metrics.active_view_measurable_impressions,
                metrics.active_view_viewability,
                metrics.all_conversions,
                metrics.all_conversions_by_conversion_date,
                metrics.all_conversions_from_interactions_rate,
                metrics.all_conversions_value,
                metrics.all_conversions_value_by_conversion_date,
                metrics.average_cost,
                metrics.average_cpc,
                metrics.average_cpe,
                metrics.average_cpm,
                metrics.average_cpv,
                metrics.average_page_views,
                metrics.average_time_on_site,
                metrics.bounce_rate,
                metrics.clicks,
                metrics.content_budget_lost_impression_share,
                metrics.content_impression_share,
                metrics.content_rank_lost_impression_share,
                metrics.conversions,
                metrics.conversions_by_conversion_date,
                metrics.conversions_from_interactions_rate,
                metrics.conversions_value,
                metrics.conversions_value_by_conversion_date,
                metrics.cost_micros,
                metrics.cost_per_all_conversions,
                metrics.cost_per_conversion,
                metrics.cost_per_current_model_attributed_conversion,
                metrics.cross_device_conversions,
                metrics.ctr,
                metrics.current_model_attributed_conversions,
                metrics.current_model_attributed_conversions_from_interactions_rate,
                metrics.current_model_attributed_conversions_from_interactions_value_per_interaction,
                metrics.current_model_attributed_conversions_value,
                metrics.current_model_attributed_conversions_value_per_cost,
                metrics.engagement_rate,
                metrics.engagements,
                metrics.gmail_forwards,
                metrics.gmail_saves,
                metrics.gmail_secondary_clicks,
                metrics.impressions,
                metrics.interaction_event_types,
                metrics.interaction_rate,
                metrics.interactions,
                metrics.invalid_click_rate,
                metrics.invalid_clicks,
                metrics.percent_new_visitors,
                metrics.phone_calls,
                metrics.phone_impressions,
                metrics.phone_through_rate,
                metrics.relative_ctr,
                metrics.search_absolute_top_impression_share,
                metrics.search_budget_lost_absolute_top_impression_share,
                metrics.search_budget_lost_impression_share,
                metrics.search_budget_lost_top_impression_share,
                metrics.search_click_share,
                metrics.search_exact_match_impression_share,
                metrics.search_impression_share,
                metrics.search_rank_lost_absolute_top_impression_share,
                metrics.search_rank_lost_impression_share,
                metrics.search_rank_lost_top_impression_share,
                metrics.search_top_impression_share,
                metrics.sk_ad_network_conversions,
                metrics.top_impression_percentage,
                metrics.value_per_all_conversions,
                metrics.value_per_all_conversions_by_conversion_date,
                metrics.value_per_conversion,
                metrics.value_per_conversions_by_conversion_date,
                metrics.value_per_current_model_attributed_conversion,
                metrics.video_quartile_p100_rate,
                metrics.video_quartile_p25_rate,
                metrics.video_quartile_p50_rate,
                metrics.video_quartile_p75_rate,
                metrics.video_view_rate,
                metrics.video_views,
                metrics.view_through_conversions,
                segments.date
            FROM campaign 
            WHERE segments.date BETWEEN '{date}' AND '{date}'"""
    elif load_type == 'clicks':
        table = '[google].[T_Clicks_newAPI_STG%s]' % OUT_SUFFIX
        query = f"""
                SELECT click_view.gclid,
                    campaign.id,
                    campaign.name,
                    ad_group.id,
                    ad_group.name,
                    click_view.ad_group_ad,
                    customer.descriptive_name,
                    segments.ad_network_type,
                    click_view.area_of_interest.city,
                    click_view.area_of_interest.metro,
                    click_view.area_of_interest.region,
                    click_view.area_of_interest.country,
                    segments.click_type,
                    metrics.clicks,
                    segments.date,
                    segments.device
                FROM click_view
                WHERE segments.date BETWEEN '{date}' AND '{date}'
        """
    else:
        raise Exception('Please enter either \'ads\', \'campaigns\', or \'clicks\' for the load_type parameter')

    search_request = client.get_type("SearchGoogleAdsRequest")
    search_request.customer_id = customer_id
    search_request.query = query
    search_request.page_size = 10000    # Max is 10,000
    results = ga_service.search(request=search_request) # Run GAQL query and store response
    # print(results)
    dictobj = MessageToDict(results._pb)    # Convert response to dict
    # print(dictobj)
    df = pd.json_normalize(dictobj,record_path=['results']) # Convert dict to DataFrame

    print(df)
    # df.to_csv(f'out_df_{date}.csv')
    
    qu.delete(table, [f'WHERE [segments_date] = \'{date}\''], config)    # Delete old records if new data was successfully retrieved
    df.insert(column='insert_at', loc=df.shape[1], value=datetime.datetime.now())   # Add insert_at column
    qu.insert(df, table, config, is_safemode=True)    # Insert new records

    print('\n\nnext_page_token:', results.next_page_token, 'Type: ', type(results.next_page_token), '\n\n')
    while results.next_page_token is not None and results.next_page_token != '':    # If exists, load next pages
        print('next_page_token found!')
        search_request.page_token = results.next_page_token
        results = ga_service.search(request=search_request)
        dictobj = MessageToDict(results._pb)
        df = pd.json_normalize(dictobj,record_path=['results'])
        # print(df)
        
        df.insert(column='insert_at', loc=df.shape[1], value=datetime.datetime.now())   # Add insert_at column
        qu.insert(df, table, config, is_safemode=True)    # Insert new records        

if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage()    # You may optionally pass the full path of the google-ads.yaml file as a parameter

    try:
        df_last_click = qu.load(f'[google].[T_Ads_newAPI_STG{OUT_SUFFIX}]', [], config, is_quick=False, columns='MAX([segments_date])')    # Find last load date from table
        if df_last_click is not None and len(df_last_click) > 0:
            last_date = datetime.datetime.strptime(df_last_click.values[0][0].split(' ')[0], '%Y-%m-%d').date()
            start_date = max(last_date - datetime.timedelta(days=ATTR_WINDOW), datetime.date.today() - datetime.timedelta(days=90)) # Google Ads API can load clicks data from up to 90 days back
        else:
            start_date = datetime.date.today() - datetime.timedelta(days=90)    # Google Ads API can load clicks data from up to 90 days back
        # start_date = datetime.date(2021, 12, 1)    # Override

        n_days = (datetime.date.today() - start_date).days + 1
        # n_days = 6  # Override
        print(f'Running {n_days} days starting from {start_date}')
        dates = [(start_date + datetime.timedelta(days=x)).strftime('%Y-%m-%d') 
                    for x in range(n_days)]

        for level in LEVELS:
            for date in tqdm(dates):
                day_wrapped = qu.wrap(main, f'Loading {level} for day {date}', config, level=2)
                day_wrapped(googleads_client, CUSTOMER_ID, date, level)
    
    except GoogleAdsException as ex:    # Error handling
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)