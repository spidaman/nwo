#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import re
import requests
from bs4 import BeautifulSoup
from collections import namedtuple
import concurrent.futures

# TODO:
# move the sites data to an external YAML file
# calculate and display the age of the observation

ObservationDatum = namedtuple('ObservationDatum', ['key', 'date', 'time', 'tws', 'gust', 'twd']) 
ObservationSite = namedtuple('ObservationSite', ['type', 'name', 'link', 'lat', 'lon', 'tz'])

glerl_pattern = '(?P<date>\\d{4}-\\d{2}-\\d{2})[ ]+(?P<time>\\d{2}:\\d{2})[ ]+(?P<tws>\\d+\\.\\d+)[ ]+(?P<gust>\\d+\\.\\d+)[ ]+(?P<twd>\\d+)'
description_element_pattern = '[^<]+<strong>(?P<key>[^<]+)</strong>(?P<value>[^<]+)<br />'
date_element_pattern = '[^<]+<strong>(?P<datetime>[^<]+)</strong><br />'

sites = {
    "atwater_park_milwaukee_wi": ObservationSite(
        "ndbc",
        "NDBC - Station 45013 - ATW20 - Atwater Park, WI Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/45013.rss",
        "43.1N",
        "87.85W",
        "CDT"
    ),
    "big_sable_point_mi": ObservationSite(
        "ndbc",
        "NDBC - Station BSBM4 - Big Sable Point, MI Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/bsbm4.rss", 
        "44.055N",
        "86.514W",
        "EDT"
    ),
    "chicago_il": ObservationSite(
        "ndbc",
        "NDBC - Station 45198 - Chicago Buoy Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/45198.rss",
        "41.892N",
        "87.563W",
        "CDT"
    ),
    "grand_traverse_light_mi": ObservationSite(
        "ndbc",
        "NDBC - Station GTLM4 - Grand Traverse Light, MI Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/gtlm4.rss", 
        "45.211N",
        "85.55W",
        "EDT"
    ),
    "little_traverse_bay_mi_buoy": ObservationSite(
        "ndbc",
        "NDBC - Station 45022 - Little Traverse Bay Buoy, MI Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/45022.rss", 
        "45.404N",
        "85.088W",
        "EDT"
    ),
    "kewaunee_wi": ObservationSite(
        "ndbc",
        "NDBC - Station KWNW3 - 9087069 - Kewaunee MET, WI Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/kwnw3.rss", 
        "44.465N",
        "87.496W",
        "CDT"
    ),
    "manistee_mi_harbor": ObservationSite(
        "ndbc",
        "NDBC - Station MEEM4 - Manistee Harbor, MI Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/meem4.rss", 
        "44.251N",
        "86.342W",
        "EDT"
    ),
    "north_lake_michigan": ObservationSite(
        "ndbc",
        "NDBC - Station 45002 - NORTH MICHIGAN- Halfway between North Manitou and Washington Islands. Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/45002.rss", 
        "45.344N",
        "86.411W",
        "EDT"
    ),
    "south_haven_mi_buoy": ObservationSite(
        "ndbc",
        "NDBC - Station 45168 - South Haven Buoy, MI Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/45002.rss", 
        "42.397N",
        "86.331W",
        "EDT"
    ),
    "south_lake_michigan": ObservationSite(
        "ndbc",
        "Station 45007 - SOUTH MICHIGAN - 43NM East Southeast of Milwaukee, WI",
        "https://www.ndbc.noaa.gov/data/latest_obs/45007.rss", 
        "42.674N",
        "87.026W",
        "EDT"
    ),
    "salmon_unlimited_wi": ObservationSite(
        "ndbc",
        "NDBC - Station 45199 - Salmon Unlimited Wisconsin Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/45199.rss", 
        "42.702N",
        "87.647W",
        "EDT"
    ),
    "sheboygan_wi": ObservationSite(
        "ndbc",
        "NDBC - Station SGNW3 - Sheboygan, WI Observations",
        "https://www.ndbc.noaa.gov/data/latest_obs/sgnw3.rss", 
        "43.749N",
        "87.693W",
        "EDT"
    ),
    "milwaukee": ObservationSite(
        "glerl",
        "NOAA/GLERL Met Station at Milwaukee, WI",
        "https://www.glerl.noaa.gov/metdata/plot_3hr.php?site=mil&units=e", 
        "43.005N",
        "87.884W",
        "CDT"
    ), 
    "south_haven": ObservationSite(
        "glerl",
        "NOAA/GLERL Met Station at South Haven, MI",
        "https://www.glerl.noaa.gov/metdata/plot_3hr.php?site=shv&units=e", 
        "42.401N",
        "86.288W",
        "CDT"
    )
}

def get_latest_ndbc_data(site_key):
    page = requests.get(sites[site_key].link)
    soup = BeautifulSoup(page.content, 'xml')
    for item in soup.find_all("item"):
        description = item.find("description").get_text()
        data = {}
        for line in description.split("\n"):
            match = re.search(description_element_pattern, line)
            if match:
                if match.group('key') == 'Wind Speed:':
                    data['tws'] = match.group('value').replace(" knots", "").strip()
                elif match.group('key') == 'Wind Gust:':
                    data['gust'] = match.group('value').replace(" knots", "").strip()
                elif match.group('key') == 'Wind Direction:':
                    data['twd'] = re.sub(r'[NEWS]+ \((\d+)&#176;\)', '\\1', match.group('value')).strip()
            else:
                match = re.search(date_element_pattern, line)
                if match:
                    dt_str = match.group('datetime')
                    if dt_str.endswith('CDT'):
                        dt_str = dt_str.replace('CDT', '-0500')
                    elif dt_str.endswith('EDT'):
                        dt_str = dt_str.replace('EDT', '-0400')
                    dt = datetime.strptime(dt_str, '%B %d, %Y %I:%M %p %z')
                    data['date'] = dt.strftime('%Y-%m-%d')
                    data['time'] = dt.strftime('%H:%M')
        tws, gust, twd = None, None, None
        try:
            tws = float(data.get('tws'))
        except:
            pass
        try:
            gust = float(data.get('gust'))
        except:
            pass
        try:
            twd = int(data.get('twd'))
        except:
            pass
        datum = ObservationDatum(site_key, data.get('date'), data.get('time'), tws, gust, twd)
        return datum


def get_latest_glerl_data(site_key):
    try:
        page = requests.get(sites[site_key].link)
    except Exception as e:
        print(f'Error parsing data from {site_key}: {e}')
        return
    soup = BeautifulSoup(page.content, 'html.parser')
    data = soup.find('pre')
    for line in data.text.split("\n"):
         if line.startswith("2025"): # fix this!
              match = re.search(glerl_pattern, line)
              if match:
                   datum = ObservationDatum(site_key, match.group('date'), match.group('time'), match.group('tws'), match.group('gust'), match.group('twd'))
                   return datum

def get_latest_site_data(site_key):
    site = sites.get(site_key)
    if site.type == "glerl":
        return get_latest_glerl_data(site_key)
    elif site.type == "ndbc":
        return get_latest_ndbc_data(site_key)
    

def get_sites():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = list(map(lambda site_key: executor.submit(get_latest_site_data, site_key), sites.keys()))
        for future in concurrent.futures.as_completed(futures):
            try:
                print(future.result())
            except Exception as e:
                print(f'Error: {e} ({type(e)})')


if __name__ == '__main__':
    #get_ndbc_sites()
    #get_glerl_sites()
    get_sites()
    