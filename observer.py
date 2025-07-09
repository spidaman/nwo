#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
from collections import namedtuple
import concurrent.futures
from datetime import datetime
import re
import requests
import sys
import yaml

# TODO:
# move the sites data to an external YAML file
# calculate and display the age of the observation

ObservationDatum = namedtuple('ObservationDatum', ['key', 'date', 'time', 'tws', 'gust', 'twd']) 
ObservationSite = namedtuple('ObservationSite', ['type', 'name', 'link', 'lat', 'lon', 'tz'])

glerl_pattern = '(?P<date>\\d{4}-\\d{2}-\\d{2})[ ]+(?P<time>\\d{2}:\\d{2})[ ]+(?P<tws>\\d+\\.\\d+)[ ]+(?P<gust>\\d+\\.\\d+)[ ]+(?P<twd>\\d+)'
description_element_pattern = '[^<]+<strong>(?P<key>[^<]+)</strong>(?P<value>[^<]+)<br />'
date_element_pattern = '[^<]+<strong>(?P<datetime>[^<]+)</strong><br />'
sites = {}

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
    else:
        print(f'type unknown for: {site}')
    

def get_sites(sites):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = list(map(lambda site_key: executor.submit(get_latest_site_data, site_key), sites.keys()))
        for future in concurrent.futures.as_completed(futures):
            try:
                print(future.result())
            except Exception as e:
                print(f'Error: {e} ({type(e)})')

def load_sites(data_file):
    with open(data_file, 'r') as data:
        site_data = yaml.load(data, Loader=yaml.FullLoader)
        for k, v in site_data.items():
            sites[k] = (ObservationSite(v.get('type'), v.get('name'), v.get('link'), v.get('lat'), v.get('lon'), v.get('tz')))
    return sites


if __name__ == '__main__':
    sites = load_sites(sys.argv[1])
    get_sites(sites)
    