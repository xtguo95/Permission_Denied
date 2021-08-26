import json
import pandas as pd
import pickle
import numpy as np

def process_travel_time(save_file_path, near_num_neighbor_out=5, further_num_neighbor_out = 5):
    with open(save_file_path + 'zone_mean_travel_times.json') as f:
        zone_mean_travel_times = json.load(f)

    zone_mean_tt_df = {'route_id':[],'from_zone':[], 'to_zone':[],'mean_travel_time':[]}
    for key in zone_mean_travel_times:
        for from_zone in zone_mean_travel_times[key]:
            for to_zone in zone_mean_travel_times[key][from_zone]:
                if (from_zone != to_zone): #and (from_zone == 'INIT'):
                    zone_mean_tt_df['route_id'].append(key)
                    zone_mean_tt_df['from_zone'].append(from_zone)
                    zone_mean_tt_df['to_zone'].append(to_zone)
                    zone_mean_tt_df['mean_travel_time'].append(zone_mean_travel_times[key][from_zone][to_zone])

    zone_mean_tt_df_out = pd.DataFrame(zone_mean_tt_df)
    #zone_mean_tt_df_out = zone_mean_tt_df_out.sort_values(['route_id','from_zone'])


    with open(save_file_path + 'zone_min_travel_times.json') as f:
        zone_mean_travel_times = json.load(f)

    zone_mean_tt_df = {'route_id':[],'from_zone':[], 'to_zone':[],'min_travel_time':[]}
    for key in zone_mean_travel_times:
        for from_zone in zone_mean_travel_times[key]:
            for to_zone in zone_mean_travel_times[key][from_zone]:
                if (from_zone != to_zone): #and (from_zone == 'INIT'):
                    zone_mean_tt_df['route_id'].append(key)
                    zone_mean_tt_df['from_zone'].append(from_zone)
                    zone_mean_tt_df['to_zone'].append(to_zone)
                    zone_mean_tt_df['min_travel_time'].append(zone_mean_travel_times[key][from_zone][to_zone])

    zone_min_tt_df_out = pd.DataFrame(zone_mean_tt_df)



    with open(save_file_path + 'zone_tt.pkl', 'wb') as f:
        pickle.dump(zone_min_tt_df_out,f)
        pickle.dump(zone_mean_tt_df_out,f)


    nearest = near_num_neighbor_out
    furtherest = further_num_neighbor_out
    zone_mean_tt_df_out = zone_mean_tt_df_out.sort_values(['route_id','from_zone','mean_travel_time'])
    zone_mean_tt_df_out_nearest5 = zone_mean_tt_df_out.groupby(['route_id','from_zone']).head(nearest)
    zone_mean_tt_df_out_nearest5.to_csv(save_file_path + 'zone_mean_tt_df_nearest_' + str(nearest) + '.csv',index=False)

    zone_mean_tt_df_out_furthest5 = zone_mean_tt_df_out.groupby(['route_id','from_zone']).tail(furtherest)
    zone_mean_tt_df_out_furthest5.to_csv(save_file_path + 'zone_mean_tt_df_furtherest_' + str(furtherest) + '.csv',index=False)

    zone_min_tt_df_out = zone_min_tt_df_out.sort_values(['route_id','from_zone','min_travel_time'])
    zone_min_tt_df_out_near = zone_min_tt_df_out.groupby(['route_id','from_zone']).head(nearest)
    zone_min_tt_df_out_near.to_csv(save_file_path + 'zone_min_tt_df_nearest_'+ str(nearest) +'.csv',index=False)

    zone_min_tt_df_out_further = zone_min_tt_df_out.groupby(['route_id','from_zone']).tail(furtherest)
    zone_min_tt_df_out_further.to_csv(save_file_path + 'zone_min_tt_df_furtherest_'+ str(furtherest) +'.csv',index=False)




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

def nearest_dist_from_init(save_file_path, near_num_neighbor_out=5, further_num_neighbor_out =5):
    all_zone = pd.read_csv(save_file_path + 'zone_data.csv')
    all_zone_ini = all_zone.loc[all_zone['zone_id'] == 'INIT'].copy().rename(columns = {'zone_id':'from_zone'})
    all_zone_neighbor = all_zone.loc[all_zone['zone_id'] != 'INIT'].copy().rename(columns = {'zone_id':'to_zone'})
    all_zone_ini_dist = all_zone_ini[['route_id','from_zone','lat_mean','lng_mean']].merge(
        all_zone_neighbor[['route_id','to_zone','lat_mean','lng_mean']], on = ['route_id'])
    all_zone_ini_dist['dist'] = haversine_np(all_zone_ini_dist['lng_mean_x'], all_zone_ini_dist['lat_mean_x'],
                                             all_zone_ini_dist['lng_mean_y'], all_zone_ini_dist['lat_mean_y'])




    nearest = near_num_neighbor_out
    furtherest = further_num_neighbor_out
    all_zone_ini_dist = all_zone_ini_dist.sort_values(['route_id','from_zone','dist'])


    all_zone_ini_dist_out = all_zone_ini_dist.loc[:,['route_id','from_zone','to_zone','dist']]
    all_zone_ini_dist_out_near = all_zone_ini_dist_out.groupby(['route_id','from_zone']).head(nearest)
    all_zone_ini_dist_out_near.to_csv(save_file_path + 'zone_ecu_dist_df_nearest_' + str(nearest) + '.csv',index=False)

    #print(all_zone_ini_dist_out_near['route_id'].iloc[6])

    zone_mean_tt_df_out_furthest = all_zone_ini_dist_out.groupby(['route_id','from_zone']).tail(furtherest)
    zone_mean_tt_df_out_furthest.to_csv(save_file_path + 'zone_ecu_dist_df_furtherest_' + str(furtherest) + '.csv',index=False)

    #a=1

def main(data_path, mode, near_num_neighbor_out=5, further_num_neighbor_out =5):

    if mode == 'build':
        data_output_path = data_path + 'model_build_outputs/'
        save_file_path = data_output_path + 'data/'
        nearest_dist_from_init(save_file_path, near_num_neighbor_out, further_num_neighbor_out)
        process_travel_time(save_file_path, near_num_neighbor_out, further_num_neighbor_out)
    if mode == 'apply':
        data_output_path =  data_path + 'model_apply_outputs/'
        save_file_path = data_output_path + 'model_apply_output/'
        nearest_dist_from_init(save_file_path, near_num_neighbor_out, further_num_neighbor_out)
        process_travel_time(save_file_path, near_num_neighbor_out, further_num_neighbor_out)


if __name__ == '__main__':
    #mode = 'build'
    mode = 'apply'
    data_path = '../data_fake/'
    if mode == 'build':
        data_output_path = data_path + 'model_build_outputs/'
        save_file_path = data_output_path + 'data/'
        nearest_dist_from_init(save_file_path, near_num_neighbor_out=5, further_num_neighbor_out=5)
        process_travel_time(save_file_path, near_num_neighbor_out=5, further_num_neighbor_out =5)
    if mode == 'apply':
        data_output_path =  data_path + 'model_apply_outputs/'
        save_file_path = data_output_path + 'model_apply_output/'
        nearest_dist_from_init(save_file_path, near_num_neighbor_out=5, further_num_neighbor_out=5)
        process_travel_time(save_file_path, near_num_neighbor_out=5, further_num_neighbor_out =5)