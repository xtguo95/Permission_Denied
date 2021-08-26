import time
import pandas as pd
import A01_process_route_seq
import A02_process_package_data
import A03_process_route_data
import numpy as np
import os

import json

def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

def merge_all_data(route_data, seq_df, package, save_file, data_output_path, data_apply_output_path, data_path):
    #tic = time.time()

    #print('load data time',time.time() - tic)

    assert len(route_data.loc[route_data['stops'].isna()]) == 0
    assert len(seq_df.loc[seq_df['stops'].isna()]) == 0
    assert len(package.loc[package['stops'].isna()]) == 0

    #%%
    route_seq = pd.merge(route_data, seq_df,on = ['route_id','stops'], how = 'left')
    route_seq = route_seq.sort_values(['route_id','seq_ID'])
    assert len(route_seq.loc[route_seq['seq_ID'].isna()]) == 0

    all_zone_id = list(pd.unique(route_seq['zone_id']))
    all_zone_id = [x for x in all_zone_id if str(x) != 'nan']
    if 'INIT' in  all_zone_id:
        all_zone_id.remove('INIT')
    replace_zone = []
    # key = all_zone_id[0]


    for key in all_zone_id:
        try:
            split_dot_1, split_dot_2 = key.split('.')
            split_dot_1_1, split_dot_1_2 =  split_dot_1.split('-')
            assert len(split_dot_1_1) == 1
            assert len(split_dot_1_2) > 0
            assert len(split_dot_2) == 2
        except:
            replace_zone.append(key)

    # check
    ###
    #z = replace_zone[1]
    all_routes_with_bad_zone = list(pd.unique(route_seq.loc[route_seq['zone_id'].isin(replace_zone),'route_id']))

    for r in all_routes_with_bad_zone:
        info = route_seq.loc[route_seq['route_id'] == r].copy()
        info_with_invalid_zone = info.loc[info['zone_id'].isin(replace_zone)]
        prop_inv_zone = len(info_with_invalid_zone)/len(info)
        if prop_inv_zone > 0.3:
            #in_valid_zone = list(pd.unique([''])) # not fill
            continue
        else:
            route_seq.loc[(route_seq['route_id'] == r) & (route_seq['zone_id'].isin(replace_zone)),'zone_id'] = np.nan


        #,'zone_id'] = np.nan

    ###
    #a1=1


    #print(route_seq['route_id'].iloc[0])
    # route_seq.loc[route_seq['route_id'] == 'RouteID_00143bdd-0a6b-49ec-bb35-36593d303e77', 'zone_id'] = np.nan

    route_seq.loc[route_seq['type'] == 'Station', 'zone_id'] = 'INIT'

    # first fill all na routes
    route_seq['zone_na'] = route_seq['zone_id'].isna()
    route_seq['max_stops_num_except_ini'] = route_seq.groupby(['route_id'])['stops'].transform('count') - 1
    route_seq['zone_na_num_stops'] = route_seq.groupby(['route_id'])['zone_na'].transform('sum')
    route_all_na = route_seq.loc[route_seq['max_stops_num_except_ini']==route_seq['zone_na_num_stops']]
    route_all_na_id = list(pd.unique(route_all_na['route_id']))
    for key in route_all_na_id:
        route_seq.loc[(route_seq['route_id'] == key)&(route_seq['zone_id'] != 'INIT'), 'zone_id'] = 'A-999.9Z'
    route_seq = route_seq.drop(columns=['zone_na','max_stops_num_except_ini','zone_na_num_stops'])
    ##############


    FILL_NA_METHOD = 'Nearest_TT_N' # NEAREST:
    if FILL_NA_METHOD == 'Nearest':
        ##########fill by nearest
        na_stop = route_seq.loc[route_seq['zone_id'].isna(),['route_id','stops','lat','lng']]

        no_na_stop = route_seq.loc[(~(route_seq['zone_id'].isna())) & (route_seq['zone_id']!='INIT'),['route_id','stops','lat','lng','zone_id']]
        no_na_stop = no_na_stop.rename(columns = {'stops':'nearby_stops','lat':'lat_nearby','lng':'lng_nearby'})
        na_stop_nearby_stop = na_stop.merge(no_na_stop, on = ['route_id'])
        na_stop_nearby_stop['dist'] = haversine_np(na_stop_nearby_stop['lng'].values, na_stop_nearby_stop['lat'].values,
                                                   na_stop_nearby_stop['lng_nearby'].values, na_stop_nearby_stop['lat_nearby'].values)
        na_stop_nearby_stop = na_stop_nearby_stop.sort_values(['route_id','stops','dist'])
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop.groupby(['route_id','stops'], as_index = False).first()
        na_stop_nearby_stop_with_zone['zone_id_infer'] = na_stop_nearby_stop_with_zone['zone_id']

        route_seq = route_seq.merge(na_stop_nearby_stop_with_zone[['route_id','stops','zone_id_infer']], on = ['route_id','stops'], how = 'left')

        route_seq.loc[route_seq['zone_id'].isna(),'zone_id'] = route_seq.loc[route_seq['zone_id'].isna(),'zone_id_infer']
        route_seq = route_seq.drop(columns = ['zone_id_infer'])
        #check_na = route_seq.loc[route_seq['zone_id'].isna()]
        #a=1
        #check_all_na = na_stop_nearby_stop.loc[na_stop_nearby_stop['']]
        a=1
        route_seq_fill_na = route_seq.copy()

    elif FILL_NA_METHOD == 'Nearest_N':
        near_num = 2
        ##########fill by nearest
        na_stop = route_seq.loc[route_seq['zone_id'].isna(),['route_id','stops','lat','lng']]

        no_na_stop = route_seq.loc[(~(route_seq['zone_id'].isna())) & (route_seq['zone_id']!='INIT'),['route_id','stops','lat','lng','zone_id']]
        no_na_stop = no_na_stop.rename(columns = {'stops':'nearby_stops','lat':'lat_nearby','lng':'lng_nearby'})
        na_stop_nearby_stop = na_stop.merge(no_na_stop, on = ['route_id'])
        na_stop_nearby_stop['dist'] = haversine_np(na_stop_nearby_stop['lng'].values, na_stop_nearby_stop['lat'].values,
                                                   na_stop_nearby_stop['lng_nearby'].values, na_stop_nearby_stop['lat_nearby'].values)

        na_stop_nearby_stop = na_stop_nearby_stop.sort_values(['route_id','stops','dist'])
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop.groupby(['route_id','stops'], as_index = False).head(near_num)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone.reset_index(drop=True)
        na_stop_nearby_stop_with_zone['weight'] = 1/ na_stop_nearby_stop_with_zone['dist']
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone.groupby(['route_id','stops','zone_id'])['weight'].sum().reset_index()
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.rename(columns = {'weight':'num_fre_zone'})
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.sort_values(['route_id','stops','num_fre_zone'],ascending=False)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone_num.groupby(['route_id','stops']).first().reset_index()
        na_stop_nearby_stop_with_zone['zone_id_infer'] = na_stop_nearby_stop_with_zone['zone_id']

        route_seq = route_seq.merge(na_stop_nearby_stop_with_zone[['route_id','stops','zone_id_infer']], on = ['route_id','stops'], how = 'left')

        route_seq.loc[route_seq['zone_id'].isna(),'zone_id'] = route_seq.loc[route_seq['zone_id'].isna(),'zone_id_infer']
        route_seq = route_seq.drop(columns = ['zone_id_infer'])
        #check_na = route_seq.loc[route_seq['zone_id'].isna()]
        #a=1
        #check_all_na = na_stop_nearby_stop.loc[na_stop_nearby_stop['']]
        a=1
        route_seq_fill_na = route_seq.copy()


    elif FILL_NA_METHOD == 'Nearest_TT_N':
        near_num = 1

        filepath = data_path + 'model_apply_inputs/new_travel_times.json'
        with open(filepath, newline='') as in_file:
            travel_time_matrix = json.load(in_file)

        stop_tt = {'route_id': [], 'from_stop': [], 'to_stop': [], 'travel_time': []}
        for key in travel_time_matrix:
            for from_zone in travel_time_matrix[key]:
                for to_zone in travel_time_matrix[key][from_zone]:
                    if (from_zone != to_zone):  # and (from_zone == 'INIT'):
                        stop_tt['route_id'].append(key)
                        stop_tt['from_stop'].append(from_zone)
                        stop_tt['to_stop'].append(to_zone)
                        stop_tt['travel_time'].append(travel_time_matrix[key][from_zone][to_zone])
        stop_tt_df = pd.DataFrame(stop_tt)



         ##########fill by nearest
        na_stop = route_seq.loc[route_seq['zone_id'].isna(), ['route_id', 'stops', 'lat', 'lng']]

        no_na_stop = route_seq.loc[
            (~(route_seq['zone_id'].isna())) & (route_seq['zone_id'] != 'INIT'), ['route_id', 'stops', 'lat', 'lng',
                                                                                  'zone_id']]
        no_na_stop = no_na_stop.rename(columns={'stops': 'nearby_stops', 'lat': 'lat_nearby', 'lng': 'lng_nearby'})
        na_stop_nearby_stop = na_stop.merge(no_na_stop, on=['route_id'])

        na_stop_nearby_stop = na_stop_nearby_stop.merge(stop_tt_df, left_on = ['route_id','stops','nearby_stops'],
                                                        right_on = ['route_id','from_stop','to_stop'])
        na_stop_nearby_stop['dist'] = na_stop_nearby_stop['travel_time']
        a=1
        # na_stop_nearby_stop['dist'] = haversine_np(na_stop_nearby_stop['lng'].values, na_stop_nearby_stop['lat'].values,
        #                                            na_stop_nearby_stop['lng_nearby'].values,
        #                                            na_stop_nearby_stop['lat_nearby'].values)

        na_stop_nearby_stop = na_stop_nearby_stop.sort_values(['route_id', 'stops', 'dist'])
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop.groupby(['route_id', 'stops'], as_index=False).head(
            near_num)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone.reset_index(drop=True)
        na_stop_nearby_stop_with_zone['weight'] = 1 / na_stop_nearby_stop_with_zone['dist']
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone.groupby(['route_id', 'stops', 'zone_id'])[
            'weight'].sum().reset_index()
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.rename(columns={'weight': 'num_fre_zone'})
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.sort_values(
            ['route_id', 'stops', 'num_fre_zone'], ascending=False)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone_num.groupby(
            ['route_id', 'stops']).first().reset_index()
        na_stop_nearby_stop_with_zone['zone_id_infer'] = na_stop_nearby_stop_with_zone['zone_id']

        route_seq = route_seq.merge(na_stop_nearby_stop_with_zone[['route_id', 'stops', 'zone_id_infer']],
                                    on=['route_id', 'stops'], how='left')

        route_seq.loc[route_seq['zone_id'].isna(), 'zone_id'] = route_seq.loc[
            route_seq['zone_id'].isna(), 'zone_id_infer']
        route_seq = route_seq.drop(columns=['zone_id_infer'])
        # check_na = route_seq.loc[route_seq['zone_id'].isna()]
        # a=1
        # check_all_na = na_stop_nearby_stop.loc[na_stop_nearby_stop['']]
        a = 1
        route_seq_fill_na = route_seq.copy()



    elif FILL_NA_METHOD == 'Nearest_TT_go_back_avg_N':
        near_num = 1
        filepath = data_path + 'model_apply_inputs/new_travel_times.json'
        with open(filepath, newline='') as in_file:
            travel_time_matrix = json.load(in_file)

        stop_tt = {'route_id': [], 'from_stop': [], 'to_stop': [], 'travel_time': []}
        for key in travel_time_matrix:
            for from_zone in travel_time_matrix[key]:
                for to_zone in travel_time_matrix[key][from_zone]:
                    if (from_zone != to_zone):  # and (from_zone == 'INIT'):
                        stop_tt['route_id'].append(key)
                        stop_tt['from_stop'].append(from_zone)
                        stop_tt['to_stop'].append(to_zone)
                        stop_tt['travel_time'].append(travel_time_matrix[key][from_zone][to_zone])
        stop_tt_df = pd.DataFrame(stop_tt)



        ##########fill by nearest
        na_stop = route_seq.loc[route_seq['zone_id'].isna(), ['route_id', 'stops', 'lat', 'lng']]

        no_na_stop = route_seq.loc[
            (~(route_seq['zone_id'].isna())) & (route_seq['zone_id'] != 'INIT'), ['route_id', 'stops', 'lat', 'lng',
                                                                                  'zone_id']]
        no_na_stop = no_na_stop.rename(columns={'stops': 'nearby_stops', 'lat': 'lat_nearby', 'lng': 'lng_nearby'})
        na_stop_nearby_stop = na_stop.merge(no_na_stop, on=['route_id'])

        ###
        na_stop_nearby_stop = na_stop_nearby_stop.merge(stop_tt_df, left_on = ['route_id','stops','nearby_stops'],
                                                        right_on = ['route_id','from_stop','to_stop'])
        na_stop_nearby_stop = na_stop_nearby_stop.rename(columns = {'travel_time':'travel_time_from_to'})
        na_stop_nearby_stop = na_stop_nearby_stop.drop(columns = ['from_stop','to_stop'])


        ##
        na_stop_nearby_stop = na_stop_nearby_stop.merge(stop_tt_df, left_on = ['route_id','nearby_stops','stops'],
                                                        right_on = ['route_id','from_stop','to_stop'])
        na_stop_nearby_stop = na_stop_nearby_stop.rename(columns = {'travel_time':'travel_time_to_from'})
        na_stop_nearby_stop = na_stop_nearby_stop.drop(columns = ['from_stop','to_stop'])

        ###
        na_stop_nearby_stop['travel_time'] = (na_stop_nearby_stop['travel_time_from_to'] + na_stop_nearby_stop['travel_time_to_from']) / 2
        # na_stop_nearby_stop['travel_time'] =  na_stop_nearby_stop['travel_time_to_from']

        na_stop_nearby_stop['dist'] = na_stop_nearby_stop['travel_time']
        a=1
        # na_stop_nearby_stop['dist'] = haversine_np(na_stop_nearby_stop['lng'].values, na_stop_nearby_stop['lat'].values,
        #                                            na_stop_nearby_stop['lng_nearby'].values,
        #                                            na_stop_nearby_stop['lat_nearby'].values)

        na_stop_nearby_stop = na_stop_nearby_stop.sort_values(['route_id', 'stops', 'dist'])
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop.groupby(['route_id', 'stops'], as_index=False).head(
            near_num)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone.reset_index(drop=True)
        na_stop_nearby_stop_with_zone['weight'] = 1 / na_stop_nearby_stop_with_zone['dist']
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone.groupby(['route_id', 'stops', 'zone_id'])[
            'weight'].sum().reset_index()
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.rename(columns={'weight': 'num_fre_zone'})
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.sort_values(
            ['route_id', 'stops', 'num_fre_zone'], ascending=False)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone_num.groupby(
            ['route_id', 'stops']).first().reset_index()
        na_stop_nearby_stop_with_zone['zone_id_infer'] = na_stop_nearby_stop_with_zone['zone_id']

        route_seq = route_seq.merge(na_stop_nearby_stop_with_zone[['route_id', 'stops', 'zone_id_infer']],
                                    on=['route_id', 'stops'], how='left')

        route_seq.loc[route_seq['zone_id'].isna(), 'zone_id'] = route_seq.loc[
            route_seq['zone_id'].isna(), 'zone_id_infer']
        route_seq = route_seq.drop(columns=['zone_id_infer'])
        # check_na = route_seq.loc[route_seq['zone_id'].isna()]
        # a=1
        # check_all_na = na_stop_nearby_stop.loc[na_stop_nearby_stop['']]
        a = 1
        route_seq_fill_na = route_seq.copy()

    if save_file:
        route_seq_fill_na.to_csv(data_output_path + 'data/build_route_with_seq.csv',index=False)

    #%%
    route_seq_package = pd.merge(route_seq_fill_na, package, on = ['route_id','stops'],how = 'left')
    route_seq_package = route_seq_package.sort_values(['route_id','seq_ID'])
    if save_file:
        route_seq_package.to_csv(data_output_path + 'data/build_route_with_seq_and_package.csv',index=False)

    return route_seq_fill_na, route_seq_package




def merge_all_data_apply(route_data, package, save_file, data_output_path, data_apply_output_path, data_path):
    #tic = time.time()

    #print('load data time',time.time() - tic)

    assert len(route_data.loc[route_data['stops'].isna()]) == 0
    assert len(package.loc[package['stops'].isna()]) == 0


    #%%
    # route_seq = pd.merge(route_data, seq_df,on = ['route_id','stops'], how = 'left')
    # route_seq = route_seq.sort_values(['route_id','seq_ID'])
    # assert len(route_seq.loc[route_seq['seq_ID'].isna()]) == 0
    route_seq = route_data.copy()



    all_zone_id = list(pd.unique(route_seq['zone_id']))
    all_zone_id = [x for x in all_zone_id if str(x) != 'nan']
    if 'INIT' in  all_zone_id:
        all_zone_id.remove('INIT')
    replace_zone = []
    # key = all_zone_id[0]
    for key in all_zone_id:
        try:
            split_dot_1, split_dot_2 = key.split('.')
            split_dot_1_1, split_dot_1_2 =  split_dot_1.split('-')
            assert len(split_dot_1_1) == 1
            assert len(split_dot_1_2) > 0
            assert len(split_dot_2) == 2
        except:
            replace_zone.append(key)

    # check
    ###
    #z = replace_zone[1]


    all_routes_with_bad_zone = list(pd.unique(route_seq.loc[route_seq['zone_id'].isin(replace_zone),'route_id']))

    for r in all_routes_with_bad_zone:
        info = route_seq.loc[route_seq['route_id'] == r].copy()
        info_with_invalid_zone = info.loc[info['zone_id'].isin(replace_zone)]
        prop_inv_zone = len(info_with_invalid_zone)/len(info)
        if prop_inv_zone > 0.3:
            #in_valid_zone = list(pd.unique([''])) # not fill
            continue
        else:
            route_seq.loc[(route_seq['route_id'] == r) & (route_seq['zone_id'].isin(replace_zone)),'zone_id'] = np.nan



    ###
    #a1=1


    #print(route_seq['route_id'].iloc[0])
    # route_seq.loc[route_seq['route_id'] == 'RouteID_00143bdd-0a6b-49ec-bb35-36593d303e77', 'zone_id'] = np.nan

    route_seq.loc[route_seq['type'] == 'Station', 'zone_id'] = 'INIT'

    # first fill all na routes
    route_seq['zone_na'] = route_seq['zone_id'].isna()
    route_seq['max_stops_num_except_ini'] = route_seq.groupby(['route_id'])['stops'].transform('count') - 1
    route_seq['zone_na_num_stops'] = route_seq.groupby(['route_id'])['zone_na'].transform('sum')
    route_all_na = route_seq.loc[route_seq['max_stops_num_except_ini']==route_seq['zone_na_num_stops']]
    route_all_na_id = list(pd.unique(route_all_na['route_id']))
    for key in route_all_na_id:
        route_seq.loc[(route_seq['route_id'] == key)&(route_seq['zone_id'] != 'INIT'), 'zone_id'] = 'A-999.9Z'
    route_seq = route_seq.drop(columns=['zone_na','max_stops_num_except_ini','zone_na_num_stops'])
    ##############


    FILL_NA_METHOD = 'Nearest_TT_N' # NEAREST:
    if FILL_NA_METHOD == 'Nearest':
        ##########fill by nearest
        na_stop = route_seq.loc[route_seq['zone_id'].isna(),['route_id','stops','lat','lng']]

        no_na_stop = route_seq.loc[(~(route_seq['zone_id'].isna())) & (route_seq['zone_id']!='INIT'),['route_id','stops','lat','lng','zone_id']]
        no_na_stop = no_na_stop.rename(columns = {'stops':'nearby_stops','lat':'lat_nearby','lng':'lng_nearby'})
        na_stop_nearby_stop = na_stop.merge(no_na_stop, on = ['route_id'])
        na_stop_nearby_stop['dist'] = haversine_np(na_stop_nearby_stop['lng'].values, na_stop_nearby_stop['lat'].values,
                                                   na_stop_nearby_stop['lng_nearby'].values, na_stop_nearby_stop['lat_nearby'].values)
        na_stop_nearby_stop = na_stop_nearby_stop.sort_values(['route_id','stops','dist'])
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop.groupby(['route_id','stops'], as_index = False).first()
        na_stop_nearby_stop_with_zone['zone_id_infer'] = na_stop_nearby_stop_with_zone['zone_id']

        route_seq = route_seq.merge(na_stop_nearby_stop_with_zone[['route_id','stops','zone_id_infer']], on = ['route_id','stops'], how = 'left')

        route_seq.loc[route_seq['zone_id'].isna(),'zone_id'] = route_seq.loc[route_seq['zone_id'].isna(),'zone_id_infer']
        route_seq = route_seq.drop(columns = ['zone_id_infer'])
        #check_na = route_seq.loc[route_seq['zone_id'].isna()]
        #a=1
        #check_all_na = na_stop_nearby_stop.loc[na_stop_nearby_stop['']]
        a=1
        route_seq_fill_na = route_seq.copy()

    elif FILL_NA_METHOD == 'Nearest_N':
        near_num = 2
        ##########fill by nearest
        na_stop = route_seq.loc[route_seq['zone_id'].isna(),['route_id','stops','lat','lng']]

        no_na_stop = route_seq.loc[(~(route_seq['zone_id'].isna())) & (route_seq['zone_id']!='INIT'),['route_id','stops','lat','lng','zone_id']]
        no_na_stop = no_na_stop.rename(columns = {'stops':'nearby_stops','lat':'lat_nearby','lng':'lng_nearby'})
        na_stop_nearby_stop = na_stop.merge(no_na_stop, on = ['route_id'])
        na_stop_nearby_stop['dist'] = haversine_np(na_stop_nearby_stop['lng'].values, na_stop_nearby_stop['lat'].values,
                                                   na_stop_nearby_stop['lng_nearby'].values, na_stop_nearby_stop['lat_nearby'].values)

        na_stop_nearby_stop = na_stop_nearby_stop.sort_values(['route_id','stops','dist'])
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop.groupby(['route_id','stops'], as_index = False).head(near_num)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone.reset_index(drop=True)
        na_stop_nearby_stop_with_zone['weight'] = 1/ na_stop_nearby_stop_with_zone['dist']
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone.groupby(['route_id','stops','zone_id'])['weight'].sum().reset_index()
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.rename(columns = {'weight':'num_fre_zone'})
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.sort_values(['route_id','stops','num_fre_zone'],ascending=False)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone_num.groupby(['route_id','stops']).first().reset_index()
        na_stop_nearby_stop_with_zone['zone_id_infer'] = na_stop_nearby_stop_with_zone['zone_id']

        route_seq = route_seq.merge(na_stop_nearby_stop_with_zone[['route_id','stops','zone_id_infer']], on = ['route_id','stops'], how = 'left')

        route_seq.loc[route_seq['zone_id'].isna(),'zone_id'] = route_seq.loc[route_seq['zone_id'].isna(),'zone_id_infer']
        route_seq = route_seq.drop(columns = ['zone_id_infer'])
        #check_na = route_seq.loc[route_seq['zone_id'].isna()]
        #a=1
        #check_all_na = na_stop_nearby_stop.loc[na_stop_nearby_stop['']]
        a=1
        route_seq_fill_na = route_seq.copy()


    elif FILL_NA_METHOD == 'Nearest_TT_N':
        near_num = 1

        filepath = data_path + 'model_apply_inputs/new_travel_times.json'
        with open(filepath, newline='') as in_file:
            travel_time_matrix = json.load(in_file)

        stop_tt = {'route_id': [], 'from_stop': [], 'to_stop': [], 'travel_time': []}
        for key in travel_time_matrix:
            for from_zone in travel_time_matrix[key]:
                for to_zone in travel_time_matrix[key][from_zone]:
                    if (from_zone != to_zone):  # and (from_zone == 'INIT'):
                        stop_tt['route_id'].append(key)
                        stop_tt['from_stop'].append(from_zone)
                        stop_tt['to_stop'].append(to_zone)
                        stop_tt['travel_time'].append(travel_time_matrix[key][from_zone][to_zone])
        stop_tt_df = pd.DataFrame(stop_tt)



         ##########fill by nearest
        na_stop = route_seq.loc[route_seq['zone_id'].isna(), ['route_id', 'stops', 'lat', 'lng']]

        no_na_stop = route_seq.loc[
            (~(route_seq['zone_id'].isna())) & (route_seq['zone_id'] != 'INIT'), ['route_id', 'stops', 'lat', 'lng',
                                                                                  'zone_id']]
        no_na_stop = no_na_stop.rename(columns={'stops': 'nearby_stops', 'lat': 'lat_nearby', 'lng': 'lng_nearby'})
        na_stop_nearby_stop = na_stop.merge(no_na_stop, on=['route_id'])

        na_stop_nearby_stop = na_stop_nearby_stop.merge(stop_tt_df, left_on = ['route_id','stops','nearby_stops'],
                                                        right_on = ['route_id','from_stop','to_stop'])
        na_stop_nearby_stop['dist'] = na_stop_nearby_stop['travel_time']
        a=1
        # na_stop_nearby_stop['dist'] = haversine_np(na_stop_nearby_stop['lng'].values, na_stop_nearby_stop['lat'].values,
        #                                            na_stop_nearby_stop['lng_nearby'].values,
        #                                            na_stop_nearby_stop['lat_nearby'].values)

        na_stop_nearby_stop = na_stop_nearby_stop.sort_values(['route_id', 'stops', 'dist'])
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop.groupby(['route_id', 'stops'], as_index=False).head(
            near_num)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone.reset_index(drop=True)
        na_stop_nearby_stop_with_zone['weight'] = 1 / na_stop_nearby_stop_with_zone['dist']
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone.groupby(['route_id', 'stops', 'zone_id'])[
            'weight'].sum().reset_index()
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.rename(columns={'weight': 'num_fre_zone'})
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.sort_values(
            ['route_id', 'stops', 'num_fre_zone'], ascending=False)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone_num.groupby(
            ['route_id', 'stops']).first().reset_index()
        na_stop_nearby_stop_with_zone['zone_id_infer'] = na_stop_nearby_stop_with_zone['zone_id']

        route_seq = route_seq.merge(na_stop_nearby_stop_with_zone[['route_id', 'stops', 'zone_id_infer']],
                                    on=['route_id', 'stops'], how='left')

        route_seq.loc[route_seq['zone_id'].isna(), 'zone_id'] = route_seq.loc[
            route_seq['zone_id'].isna(), 'zone_id_infer']
        route_seq = route_seq.drop(columns=['zone_id_infer'])
        # check_na = route_seq.loc[route_seq['zone_id'].isna()]
        # a=1
        # check_all_na = na_stop_nearby_stop.loc[na_stop_nearby_stop['']]
        a = 1
        route_seq_fill_na = route_seq.copy()



    elif FILL_NA_METHOD == 'Nearest_TT_go_back_avg_N':
        near_num = 1
        filepath = data_path + 'model_apply_inputs/new_travel_times.json'
        with open(filepath, newline='') as in_file:
            travel_time_matrix = json.load(in_file)

        stop_tt = {'route_id': [], 'from_stop': [], 'to_stop': [], 'travel_time': []}
        for key in travel_time_matrix:
            for from_zone in travel_time_matrix[key]:
                for to_zone in travel_time_matrix[key][from_zone]:
                    if (from_zone != to_zone):  # and (from_zone == 'INIT'):
                        stop_tt['route_id'].append(key)
                        stop_tt['from_stop'].append(from_zone)
                        stop_tt['to_stop'].append(to_zone)
                        stop_tt['travel_time'].append(travel_time_matrix[key][from_zone][to_zone])
        stop_tt_df = pd.DataFrame(stop_tt)



        ##########fill by nearest
        na_stop = route_seq.loc[route_seq['zone_id'].isna(), ['route_id', 'stops', 'lat', 'lng']]

        no_na_stop = route_seq.loc[
            (~(route_seq['zone_id'].isna())) & (route_seq['zone_id'] != 'INIT'), ['route_id', 'stops', 'lat', 'lng',
                                                                                  'zone_id']]
        no_na_stop = no_na_stop.rename(columns={'stops': 'nearby_stops', 'lat': 'lat_nearby', 'lng': 'lng_nearby'})
        na_stop_nearby_stop = na_stop.merge(no_na_stop, on=['route_id'])

        ###
        na_stop_nearby_stop = na_stop_nearby_stop.merge(stop_tt_df, left_on = ['route_id','stops','nearby_stops'],
                                                        right_on = ['route_id','from_stop','to_stop'])
        na_stop_nearby_stop = na_stop_nearby_stop.rename(columns = {'travel_time':'travel_time_from_to'})
        na_stop_nearby_stop = na_stop_nearby_stop.drop(columns = ['from_stop','to_stop'])


        ##
        na_stop_nearby_stop = na_stop_nearby_stop.merge(stop_tt_df, left_on = ['route_id','nearby_stops','stops'],
                                                        right_on = ['route_id','from_stop','to_stop'])
        na_stop_nearby_stop = na_stop_nearby_stop.rename(columns = {'travel_time':'travel_time_to_from'})
        na_stop_nearby_stop = na_stop_nearby_stop.drop(columns = ['from_stop','to_stop'])

        ###
        na_stop_nearby_stop['travel_time'] = (na_stop_nearby_stop['travel_time_from_to'] + na_stop_nearby_stop['travel_time_to_from']) / 2
        # na_stop_nearby_stop['travel_time'] =  na_stop_nearby_stop['travel_time_to_from']

        na_stop_nearby_stop['dist'] = na_stop_nearby_stop['travel_time']
        a=1
        # na_stop_nearby_stop['dist'] = haversine_np(na_stop_nearby_stop['lng'].values, na_stop_nearby_stop['lat'].values,
        #                                            na_stop_nearby_stop['lng_nearby'].values,
        #                                            na_stop_nearby_stop['lat_nearby'].values)

        na_stop_nearby_stop = na_stop_nearby_stop.sort_values(['route_id', 'stops', 'dist'])
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop.groupby(['route_id', 'stops'], as_index=False).head(
            near_num)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone.reset_index(drop=True)
        na_stop_nearby_stop_with_zone['weight'] = 1 / na_stop_nearby_stop_with_zone['dist']
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone.groupby(['route_id', 'stops', 'zone_id'])[
            'weight'].sum().reset_index()
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.rename(columns={'weight': 'num_fre_zone'})
        na_stop_nearby_stop_with_zone_num = na_stop_nearby_stop_with_zone_num.sort_values(
            ['route_id', 'stops', 'num_fre_zone'], ascending=False)
        na_stop_nearby_stop_with_zone = na_stop_nearby_stop_with_zone_num.groupby(
            ['route_id', 'stops']).first().reset_index()
        na_stop_nearby_stop_with_zone['zone_id_infer'] = na_stop_nearby_stop_with_zone['zone_id']

        route_seq = route_seq.merge(na_stop_nearby_stop_with_zone[['route_id', 'stops', 'zone_id_infer']],
                                    on=['route_id', 'stops'], how='left')

        route_seq.loc[route_seq['zone_id'].isna(), 'zone_id'] = route_seq.loc[
            route_seq['zone_id'].isna(), 'zone_id_infer']
        route_seq = route_seq.drop(columns=['zone_id_infer'])
        # check_na = route_seq.loc[route_seq['zone_id'].isna()]
        # a=1
        # check_all_na = na_stop_nearby_stop.loc[na_stop_nearby_stop['']]
        a = 1
        route_seq_fill_na = route_seq.copy()

    if save_file:
        route_seq_fill_na.to_csv(data_apply_output_path + 'model_apply_output/build_route_with_seq.csv',index=False)

    #%%
    route_seq_package = pd.merge(route_seq_fill_na, package, on = ['route_id','stops'],how = 'left')
    route_seq_package = route_seq_package.sort_values(['route_id'])
    if save_file:
        route_seq_package.to_csv(data_apply_output_path + 'model_apply_output/build_route_with_seq_and_package.csv',index=False)

    return route_seq_fill_na, route_seq_package


def get_all_data(data_path, read_file, save_file, previous_save_file, mode,
                 data_output_path, data_apply_output_path):
    if mode == 'build':
        if read_file:
            route_data_build = pd.read_csv(data_output_path + 'data/build_route_df.csv')
            route_data_build['stops'] = route_data_build['stops'].fillna('NA')

            seq_df = pd.read_csv(data_output_path + 'data/build_route_seq_df.csv')
            seq_df['stops'] = seq_df['stops'].fillna('NA')

            package_df_build = pd.read_csv(data_output_path + 'data/build_package_df.csv')
            package_df_build['stops'] = package_df_build['stops'].fillna('NA')
            #a=1
        else:
            #if mode == 'build':
            seq_df = A01_process_route_seq.generate_build_route_seq_df(data_path, save_file = previous_save_file,
                                                                       data_output_path = data_output_path)
            package_df_build = A02_process_package_data.generate_package_data(data_path, save_file = previous_save_file,
                                                                              mode = mode, data_output_path = data_output_path,
                                                                              data_apply_output_path = data_apply_output_path)
            route_data_build = A03_process_route_data.generate_route_df(data_path, save_file = previous_save_file,
                                                                        mode = mode, data_output_path = data_output_path,
                                                                        data_apply_output_path = data_apply_output_path)
        route_seq_fill_na, route_seq_package = merge_all_data(route_data_build, seq_df, package_df_build, save_file,
                                                              data_output_path, data_apply_output_path, data_path)
    elif mode == 'apply':
        #route_data_build =
        package_df_build = A02_process_package_data.generate_package_data(data_path, save_file=previous_save_file,
                                                                          mode=mode, data_output_path = data_output_path,
                                                                          data_apply_output_path = data_apply_output_path)
        route_data_build = A03_process_route_data.generate_route_df(data_path, save_file=previous_save_file, mode=mode,
                                                                    data_output_path = data_output_path,
                                                                    data_apply_output_path = data_apply_output_path)

        # a=1
        route_seq_fill_na, route_seq_package = merge_all_data_apply(route_data_build, package_df_build, save_file,
                                                                    data_output_path = data_output_path,
                                                                    data_apply_output_path = data_apply_output_path,
                                                                    data_path = data_path)
    return route_seq_fill_na, route_seq_package


def main(data_path, mode):
    if mode == 'build':
        save_file = True
        read_file = False
        previous_save_file = True
        data_output_path = data_path + 'model_build_outputs/'
        data_apply_output_path = None

        if not os.path.exists(data_output_path + 'data/'):
            os.mkdir(data_output_path + 'data/')

        if not os.path.exists(data_output_path + 'data/mean_dist/'):
            os.mkdir(data_output_path + 'data/mean_dist/')

    elif mode == 'apply':
        save_file = True
        read_file = False
        previous_save_file = True
        data_output_path = data_path + 'model_build_outputs/'
        data_apply_output_path = data_path + 'model_apply_outputs/'

        if not os.path.exists(data_apply_output_path + 'model_apply_output/'):
            os.mkdir(data_apply_output_path + 'model_apply_output/')

        if not os.path.exists(data_output_path + 'model_apply_output/mean_dist/'):
            os.mkdir(data_output_path + 'model_apply_output/mean_dist/')

    tic = time.time()
    get_all_data(data_path, read_file, save_file, previous_save_file, mode = mode,
                 data_output_path = data_output_path, data_apply_output_path = data_apply_output_path)
    print('generate file time', time.time() - tic)

if __name__ == '__main__':
    # route_data = pd.read_csv('../data/build_route_df.csv', keep_default_na=False)
    # seq_df = pd.read_csv('../data/build_route_seq_df.csv', keep_default_na=False)
    # package = pd.read_csv('../data/build_package_df.csv', keep_default_na=False)

    mode = 'build'
    if mode == 'build':
        data_path = '../data_fake/'
        save_file = True
        read_file = False
        previous_save_file = True
        data_output_path = '../data_fake/model_build_outputs/'
        data_apply_output_path = None

        if not os.path.exists(data_output_path + 'data/'):
            os.mkdir(data_output_path + 'data/')


    elif mode == 'apply':
        data_path = '../data_fake/'
        save_file = True
        read_file = False
        previous_save_file = True
        data_output_path = '../data_fake/model_build_outputs/'
        data_apply_output_path = '../data_fake/model_apply_outputs/'

        if not os.path.exists(data_apply_output_path + 'model_apply_output/'):
            os.mkdir(data_apply_output_path + 'model_apply_output/')

    tic = time.time()
    get_all_data(data_path, read_file, save_file, previous_save_file, mode = mode,
                 data_output_path = data_output_path, data_apply_output_path = data_apply_output_path)
    print('generate file time', time.time() - tic)

