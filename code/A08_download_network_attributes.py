def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn # what an elegant way to avoid the fucking warnings!
import numpy as np
import pandas as pd
#import folium
import json
import time
import networkx as nx
import osmnx as ox
import osm2gmns as og
import matplotlib.pyplot as plt
import os


def download_highway_link_for_all_cities(save_dir, station_file, stop_all, num_km_extended):

    if not os.path.exists(data_output_path + 'data/'):
        os.mkdir(data_output_path + 'data/')

    list_station = list(station_file['station_code'])
    station_dict = {}
    for key in list_station:
        station_city = key[1:-1]
        if station_city not in station_dict:
            station_dict[station_city] = [key]
        else:
            station_dict[station_city].append(key)

    tic = time.time()

    for key in station_dict:
        for station in station_dict[key]:
            print('city',key,'station',station)
            stop_city = stop_all.loc[stop_all['station_code'] == station].copy()
            min_lat = stop_city['lat'].min() - num_km_extended / 111
            min_lng = stop_city['lng'].min() - num_km_extended / 111
            max_lat = stop_city['lat'].max() + num_km_extended / 111
            max_lng = stop_city['lng'].max() + num_km_extended / 111
            stime = time.time()
            G = ox.graph.graph_from_bbox(north=max_lat, south=min_lat, east=max_lng,
                                          west=min_lng, network_type='drive',
                                          simplify=True, custom_filter='["highway"~"primary_link|secondary_link"]')
            print(station, "build graph time", time.time() - stime)

            ox.save_graph_xml(G, filepath=save_dir + station + '_primary.osm')
            net = og.getNetFromOSMFile(save_dir + station + '_primary.osm')
            og.consolidateComplexIntersections(net)
            directory = save_dir + station + 'net/primary_and_secondary/'


            if not os.path.exists(directory):
                os.makedirs(directory)

            og.outputNetToCSV(net, output_folder = directory)
    print('total download net time',time.time() - tic)


def merge_net_to_zone_data(data_path):
    zone_data_path = data_path + 'model_build_outputs/data/'
    zone_data = pd.read_csv(zone_data_path + 'zone_data.csv')
    ###################
    station_location = pd.read_csv(zone_data_path + 'station_location.csv')
    station_list = list(station_location['station_code'])
    sig_station = []
    for station in station_list:
        link = pd.read_csv(zone_data_path + station + 'net/primary_and_secondary/link.csv')
        link_info = link[['link_id','geometry']]
        temp = link_info['geometry'].str.split()
        nodes = nodes.loc[:, ['node_id', 'osm_highway', 'x_coord', 'y_coord']]
        # cross = nodes.loc[nodes['osm_highway'] == 'crossing']
        signals = nodes.loc[nodes['osm_highway'] == 'traffic_signals'].copy()
        signals['station_code'] = station
        sig_station.append(signals)
        a = 1

    sig_station = pd.concat(sig_station)
    data_zone['key'] = 1
    sig_station['key'] = 1
    data_zone_sig = data_zone.merge(sig_station, on=['key', 'station_code'])
    data_zone_sig['dist'] = haversine_np(data_zone_sig['lng_mean'], data_zone_sig['lat_mean'], data_zone_sig['x_coord'],
                                         data_zone_sig['y_coord'])
    #
    nearby = 0.5  # 5km
    #
    data_zone_sig = data_zone_sig.loc[data_zone_sig['dist'] <= nearby]
    data_zone_sig_num = data_zone_sig.groupby(['route_id', 'zone_id'])['osm_highway'].count().reset_index()
    data_zone_sig_num = data_zone_sig_num.rename(columns={'osm_highway': 'num_tra_sig'})
    data_zone = data_zone.merge(data_zone_sig_num, on=['route_id', 'zone_id'], how='left')
    data_zone['num_tra_sig'] = data_zone['num_tra_sig'].fillna(0)


    a=1

if __name__ == '__main__':
    #################
    # station_file = pd.read_csv('../data/station_location.csv')
    # stop_all = pd.read_csv('../data/build_route_with_seq.csv')
    # num_km_extended = 0
    # save_dir = ''
    # download_highway_link_for_all_cities(save_dir, station_file, stop_all, num_km_extended)
    ################
    data_path = '../data_fake/'
    merge_net_to_zone_data(data_path)
