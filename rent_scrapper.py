import requests as req
import bs4
import json
import pandas as pd
import datetime
import pickle
from googleapiclient.discovery import build
import base64
import re
import time
import argparse
import googlemaps
import configparser

config = configparser.ConfigParser()
config.read("secrets/config.ini")
API_key = config['Keys']['google_API']
gmaps = googlemaps.Client(key=API_key)

critical_address_gps = {'cole_home': {'gps':[51.1191706,-114.2229905], 'mode': 'transit', 'departure_time':datetime.datetime(2020,4,25,17)},
                       'tiff_home': {'gps': [50.9751225,-114.0240217], 'mode': 'driving', 'departure_time':datetime.datetime(2020,4,25,17)},
                       'acadia_clinic': {'gps': [50.9602494,-114.0471263,17], 'mode': 'driving', 'departure_time':datetime.datetime(2020,4,27,8)},
                       'wildlife': {'gps': [51.1575318,-114.2178905], 'mode': 'driving', 'departure_time':datetime.datetime(2020,4,30,8)}
                       }


def extract_travel_times(directions):
    travel_times = {}
    for s in directions[0]['legs'][0]['steps']:
        if s['travel_mode'] in travel_times.keys():
            travel_times[s['travel_mode']] = travel_times[s['travel_mode']] + s['duration']['value']
        else:
            travel_times[s['travel_mode']] = s['duration']['value']
    return travel_times

def get_critical_travel_times(lat, lon, critical_address_gps):
    time_data = {}
    for a, d_config in critical_address_gps.items():
        directs = gmaps.directions(origin = [lat, lon],
                                   destination = d_config['gps'], mode = d_config['mode'],
                                   departure_time = d_config['departure_time'],
                                   alternatives = False)

        times = extract_travel_times(directs)
        for mode, t_val in times.items():
            data_col = f'{a}_{mode}'
            time_data[data_col] = t_val
    return time_data

def cache_listings(save_path, max_emails = 1):
    df = pd.read_csv(save_path)
    for rental_url in get_rental_ids(max_emails):
        df = add_listing_data(df, rental_url)
        
    df.to_csv(save_path, index = False)
    
def get_rental_ids(max_emails):
    rental_ids = []
    cred_path = 'secrets/gmail_token.pickle'
    with open(cred_path, 'rb') as pickle_token:
        user_credentials = pickle.load(pickle_token)

    gmail_app = build("gmail", 'v1', credentials=user_credentials, cache_discovery=False)
    emails = gmail_app.users().messages().list(userId='me', maxResults=max_emails,
                                           labelIds="Label_400557737346208601").execute()['messages']
    for message in emails:
        payload = gmail_app.users().messages().get(userId='me', id=message['id']).execute()['payload']
        htmlpart = payload['parts'][1]['body']['data']
        email_html =  base64.urlsafe_b64decode(htmlpart.encode('UTF-8')).decode('iso-8859-1')
        regex = re.compile("https://www.rentfaster.ca/[0-9]{6}")
        rental_ids.extend(regex.findall(email_html))
    return set(rental_ids)

def add_listing_data(full_df, listing_url):
    if full_df.shape[0] > 0:
        cached_listings = set(full_df['listing_url'])
    else:
        cached_listings = {}
        
    if listing_url not in cached_listings:
        try:
            print(f"collecting data for listing {listing_url}")
            features = []
            resp = req.get(listing_url)
            time.sleep(60)
            soup = bs4.BeautifulSoup(resp.text, 'html.parser')

            description = soup.find(id="listingview_full_desc").text.strip()
            lat = soup.find(property="rentfaster:location:latitude").get("content")
            lon = soup.find(property="rentfaster:location:longitude").get("content")
            for feat in soup.findAll(property="amenityFeature") + soup.findAll(property="additionalProperty"):
                features.append(feat.text.strip())

            for s in soup.findAll('script'):
                if 'window.units =' in s.text:
                    stats_str = "[" + s.text.strip().split("[")[1]
                    stats_str = stats_str.split("]")[0] + "]"
                    stats = json.loads(stats_str)[-1]
            
            time_data = get_critical_travel_times(lat, lon, critical_address_gps)

            df = pd.DataFrame()
            df['timestamp'] = [datetime.datetime.now()]
            df['listing_url'] = listing_url
            df['lat'] = lat
            df['lon'] = lon
            df['description'] = description
            for key, val in stats.items():
                df[key] = val

            for feat in features:
                df[feat]= 1
            
            for time_col, t in time_data.items():
                df[time_col] = t
        
            return full_df.append(df)
        except Exception as e:
            print(f"Failed to parse {listing_url} error = {str(e)}")
            return full_df
    return full_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='CLI interface for scrappind rentfaster data')
    parser.add_argument('--save-path', action='store', dest='save_path',
                        help='path to save results to. If file exists, results are appended')
    parser.add_argument('--max-emails', action='store', default=1, dest='max_emails',
                        help='the maximum number of emails to query')
    args = parser.parse_args()

    cache_listings(args.save_path, args.max_emails)