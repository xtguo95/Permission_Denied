import pandas as pd
import numpy as np
# from datetime import datetime
# from tzwhere import tzwhere
# from dateutil import tz
# import pytz




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



def generate_zone_att(data, save_file, data_output_path):

    data['stops'] = data['stops'].fillna('NA')
    data.loc[data['type'] == 'Station', 'zone_id'] = 'INIT'
    data['zone_id'] = data['zone_id'].fillna(method='bfill')
    data['zone_id'] = data['zone_id'].fillna(method='ffill')
    # data = data.dropna(subset = ['zone_id'])

    data['total_vol'] = data['depth_cm'] * data['height_cm'] * data['width_cm']

    # data['num_pkg'] = data.groupby([''])

    data['max_dhw'] = np.maximum(np.maximum(data['depth_cm'].values, data['height_cm'].values), data['width_cm'].values)
    data['time_window_end_dt'] = pd.to_datetime(data['time_window_end'], format='%Y-%m-%d %H:%M:%S')
    data['departure_date_time'] = data['date'] + ' ' + data['departure_time']
    data['departure_date_time_dt'] = pd.to_datetime(data['departure_date_time'], format='%Y-%m-%d %H:%M:%S')
    data['time_window_end_from_departure_sec'] = data['time_window_end_dt'] - data['departure_date_time_dt']
    data['time_window_end_from_departure_sec'] = data['time_window_end_from_departure_sec'].dt.total_seconds()
    data['time_window_end_from_departure_sec'] = data['time_window_end_from_departure_sec'].fillna(99999)

    data_stops = data[['route_id', 'zone_id', 'stops', 'lat', 'lng']].drop_duplicates()
    data_stops['total_num_stops_per_zone'] = data_stops.groupby(['route_id', 'zone_id'])['stops'].transform('count')
    data_stops['lat_mean'] = data_stops.groupby(['route_id', 'zone_id'])['lat'].transform('mean')
    data_stops['lng_mean'] = data_stops.groupby(['route_id', 'zone_id'])['lng'].transform('mean')
    data_stops = data_stops[
        ['route_id', 'zone_id', 'lat_mean', 'lng_mean', 'total_num_stops_per_zone']].drop_duplicates()

    data['n_pkg'] = data.groupby(['route_id', 'zone_id'])['pack_ID'].transform('count')




    col_to_group = ['route_id', 'zone_id', 'station_code', 'departure_date_time', 'exe_cap_cm3', 'route_score', 'n_pkg']

    data_zone = data.groupby(col_to_group, sort=False).agg({'planned_service_time': ['sum'],
                                                            'depth_cm': ['max', 'mean', 'sum'],
                                                            'height_cm': ['max', 'mean', 'sum'],
                                                            'width_cm': ['max', 'mean', 'sum'],
                                                            'total_vol': ['max', 'mean', 'sum'],
                                                            'max_dhw': ['max', 'mean', 'sum'],
                                                            'time_window_end_from_departure_sec': [
                                                                'min']}).reset_index()

    data_zone.columns = data_zone.columns = ['_'.join(col).strip() for col in data_zone.columns.values]

    for col in col_to_group:
        data_zone = data_zone.rename(columns={col + '_': col})

    data_zone = data_zone.merge(data_stops, on=['route_id', 'zone_id'])


    ################
    ## get num tra sigs
    # station_location = pd.read_csv('../data/station_location.csv')
    # station_list = list(station_location['station_code'])
    # sig_station = []
    # for station in station_list:
    #     nodes = pd.read_csv('../baichuan_ML_test/' + station + 'net/primary/node.csv')
    #     nodes = nodes.loc[:, ['node_id', 'osm_highway', 'x_coord', 'y_coord']]
    #     # cross = nodes.loc[nodes['osm_highway'] == 'crossing']
    #     signals = nodes.loc[nodes['osm_highway'] == 'traffic_signals'].copy()
    #     signals['station_code'] = station
    #     sig_station.append(signals)
    #     a = 1
    #
    # sig_station = pd.concat(sig_station)
    # data_zone['key'] = 1
    # sig_station['key'] = 1
    # data_zone_sig = data_zone.merge(sig_station, on=['key', 'station_code'])
    # data_zone_sig['dist'] = haversine_np(data_zone_sig['lng_mean'], data_zone_sig['lat_mean'], data_zone_sig['x_coord'],
    #                                      data_zone_sig['y_coord'])
    # #
    # nearby = 0.5  # 5km
    # #
    # data_zone_sig = data_zone_sig.loc[data_zone_sig['dist'] <= nearby]
    # data_zone_sig_num = data_zone_sig.groupby(['route_id', 'zone_id'])['osm_highway'].count().reset_index()
    # data_zone_sig_num = data_zone_sig_num.rename(columns={'osm_highway': 'num_tra_sig'})
    # data_zone = data_zone.merge(data_zone_sig_num, on=['route_id', 'zone_id'], how='left')
    # data_zone['num_tra_sig'] = data_zone['num_tra_sig'].fillna(0)
    ###############


    #########################
    ### calculate local time
    # tz_func = tzwhere.tzwhere()
    #
    # station_unique = data_zone.drop_duplicates(['station_code']).copy()
    # station_unique['time_zone'] = station_unique[['lat_mean', 'lng_mean']].apply(lambda x: tz_func.tzNameAt(x[0], x[1]),
    #                                                                              axis=1)
    #
    # # from_zone = tz.gettz('UTC')
    #
    # data_zone['zone_seq'] = data_zone.groupby(['route_id'], sort=False).cumcount() + 1
    #
    # time_diff_list = [0] * len(station_unique)
    # count = 0
    #
    # for idx, row in station_unique.iterrows():
    #     time_zone_pytz = pytz.timezone(row['time_zone'])
    #     time_diff = time_zone_pytz.utcoffset(datetime(2018, 7, 30))
    #     time_diff_list[count] = time_diff
    #     count += 1
    #
    # station_unique['time_diff'] = time_diff_list
    #
    # data_zone = data_zone.merge(station_unique[['station_code', 'time_zone', 'time_diff']], on=['station_code'])
    #
    # data_zone['departure_date_time_dt'] = pd.to_datetime(data_zone['departure_date_time'], format='%Y-%m-%d %H:%M:%S')
    # data_zone['departure_date_time_local'] = data_zone['departure_date_time_dt'] + data_zone['time_diff']
    # data_zone['day_of_week'] = data_zone['departure_date_time_local'].dt.dayofweek  # Monday 0 Sunday 6
    # data_zone['hour'] = data_zone['departure_date_time_local'].dt.hour
    #
    # # data_zone['hour'].hist()
    #
    # data_zone['departure_date_time_local'] = data_zone['departure_date_time_local'].astype("str")
    # # data_zone = data_zone.fillna(0)
    # data_zone = data_zone.drop(columns=['departure_date_time_dt', 'time_diff', 'time_zone'])
    #############################



    if save_file:
        data_zone.to_csv(data_output_path + 'data/zone_data.csv',index = False)


    return data_zone




def generate_zone_att_apply(data, save_file, data_apply_output_path):
    data['stops'] = data['stops'].fillna('NA')
    data.loc[data['type'] == 'Station', 'zone_id'] = 'INIT'
    data['zone_id'] = data['zone_id'].fillna(method='bfill')
    data['zone_id'] = data['zone_id'].fillna(method='ffill')
    # data = data.dropna(subset = ['zone_id'])

    data['total_vol'] = data['depth_cm'] * data['height_cm'] * data['width_cm']

    # data['num_pkg'] = data.groupby([''])

    data['max_dhw'] = np.maximum(np.maximum(data['depth_cm'].values, data['height_cm'].values), data['width_cm'].values)
    data['time_window_end_dt'] = pd.to_datetime(data['time_window_end'], format='%Y-%m-%d %H:%M:%S')
    data['departure_date_time'] = data['date'] + ' ' + data['departure_time']
    data['departure_date_time_dt'] = pd.to_datetime(data['departure_date_time'], format='%Y-%m-%d %H:%M:%S')
    data['time_window_end_from_departure_sec'] = data['time_window_end_dt'] - data['departure_date_time_dt']
    data['time_window_end_from_departure_sec'] = data['time_window_end_from_departure_sec'].dt.total_seconds()
    data['time_window_end_from_departure_sec'] = data['time_window_end_from_departure_sec'].fillna(99999)

    data_stops = data[['route_id', 'zone_id', 'stops', 'lat', 'lng']].drop_duplicates()
    data_stops['total_num_stops_per_zone'] = data_stops.groupby(['route_id', 'zone_id'])['stops'].transform('count')
    data_stops['lat_mean'] = data_stops.groupby(['route_id', 'zone_id'])['lat'].transform('mean')
    data_stops['lng_mean'] = data_stops.groupby(['route_id', 'zone_id'])['lng'].transform('mean')
    data_stops = data_stops[
        ['route_id', 'zone_id', 'lat_mean', 'lng_mean', 'total_num_stops_per_zone']].drop_duplicates()

    data['n_pkg'] = data.groupby(['route_id', 'zone_id'])['pack_ID'].transform('count')


    col_to_group = ['route_id', 'zone_id', 'station_code', 'departure_date_time', 'exe_cap_cm3', 'n_pkg']

    data_zone = data.groupby(col_to_group, sort=False).agg({'planned_service_time': ['sum'],
                                                            'depth_cm': ['max', 'mean', 'sum'],
                                                            'height_cm': ['max', 'mean', 'sum'],
                                                            'width_cm': ['max', 'mean', 'sum'],
                                                            'total_vol': ['max', 'mean', 'sum'],
                                                            'max_dhw': ['max', 'mean', 'sum'],
                                                            'time_window_end_from_departure_sec': [
                                                                'min']}).reset_index()

    data_zone.columns = data_zone.columns = ['_'.join(col).strip() for col in data_zone.columns.values]

    for col in col_to_group:
        data_zone = data_zone.rename(columns={col + '_': col})

    data_zone = data_zone.merge(data_stops, on=['route_id', 'zone_id'])


    ###################
    # station_location = pd.read_csv('../data/station_location.csv')
    # station_list = list(station_location['station_code'])
    # sig_station = []
    # for station in station_list:
    #     nodes = pd.read_csv('../baichuan_ML_test/' + station + 'net/primary/node.csv')
    #     nodes = nodes.loc[:, ['node_id', 'osm_highway', 'x_coord', 'y_coord']]
    #     # cross = nodes.loc[nodes['osm_highway'] == 'crossing']
    #     signals = nodes.loc[nodes['osm_highway'] == 'traffic_signals'].copy()
    #     signals['station_code'] = station
    #     sig_station.append(signals)
    #     a = 1
    #
    # sig_station = pd.concat(sig_station)
    # data_zone['key'] = 1
    # sig_station['key'] = 1
    # data_zone_sig = data_zone.merge(sig_station, on=['key', 'station_code'])
    # data_zone_sig['dist'] = haversine_np(data_zone_sig['lng_mean'], data_zone_sig['lat_mean'], data_zone_sig['x_coord'],
    #                                      data_zone_sig['y_coord'])
    # #
    # nearby = 0.5  # 5km
    # #
    # data_zone_sig = data_zone_sig.loc[data_zone_sig['dist'] <= nearby]
    # data_zone_sig_num = data_zone_sig.groupby(['route_id', 'zone_id'])['osm_highway'].count().reset_index()
    # data_zone_sig_num = data_zone_sig_num.rename(columns={'osm_highway': 'num_tra_sig'})
    # data_zone = data_zone.merge(data_zone_sig_num, on=['route_id', 'zone_id'], how='left')
    # data_zone['num_tra_sig'] = data_zone['num_tra_sig'].fillna(0)
    #####################


    #########################
    ### calculate local time
    # tz_func = tzwhere.tzwhere()
    #
    # station_unique = data_zone.drop_duplicates(['station_code']).copy()
    # station_unique['time_zone'] = station_unique[['lat_mean', 'lng_mean']].apply(lambda x: tz_func.tzNameAt(x[0], x[1]),
    #                                                                              axis=1)
    #
    # # from_zone = tz.gettz('UTC')
    #
    #
    # time_diff_list = [0] * len(station_unique)
    # count = 0
    #
    # for idx, row in station_unique.iterrows():
    #     time_zone_pytz = pytz.timezone(row['time_zone'])
    #     time_diff = time_zone_pytz.utcoffset(datetime(2018, 7, 30))
    #     time_diff_list[count] = time_diff
    #     count += 1
    #
    # station_unique['time_diff'] = time_diff_list
    #
    # data_zone = data_zone.merge(station_unique[['station_code', 'time_zone', 'time_diff']], on=['station_code'])
    #
    # data_zone['departure_date_time_dt'] = pd.to_datetime(data_zone['departure_date_time'], format='%Y-%m-%d %H:%M:%S')
    # data_zone['departure_date_time_local'] = data_zone['departure_date_time_dt'] + data_zone['time_diff']
    # data_zone['day_of_week'] = data_zone['departure_date_time_local'].dt.dayofweek  # Monday 0 Sunday 6
    # data_zone['hour'] = data_zone['departure_date_time_local'].dt.hour
    #
    # # data_zone['hour'].hist()
    #
    # data_zone['departure_date_time_local'] = data_zone['departure_date_time_local'].astype("str")
    # # data_zone = data_zone.fillna(0)
    # data_zone = data_zone.drop(columns=['departure_date_time_dt', 'time_diff', 'time_zone'])

    ##############################

    if save_file:
        data_zone.to_csv(data_apply_output_path + 'model_apply_output/zone_data.csv',index = False)


    return data_zone

def main(data_path, mode):
    save_file = True

    if mode == 'build':
        data_output_path = data_path + 'model_build_outputs/'
        data = pd.read_csv(data_output_path + 'data/build_route_with_seq_and_package.csv')
        generate_zone_att(data, save_file, data_output_path)
    elif mode == 'apply':
        data_apply_output_path = data_path + 'model_apply_outputs/'
        data = pd.read_csv(data_apply_output_path + 'model_apply_output/build_route_with_seq_and_package.csv')
        generate_zone_att_apply(data, save_file, data_apply_output_path)


if __name__ == '__main__':
    save_file = True

    mode = 'apply'
    if mode == 'build':
        data_output_path = '../data_fake/model_build_outputs/'
        data = pd.read_csv(data_output_path + 'data/build_route_with_seq_and_package.csv')
        generate_zone_att(data, save_file, data_output_path)
    elif mode == 'apply':
        data_apply_output_path = '../data_fake/model_apply_outputs/'
        data = pd.read_csv(data_apply_output_path + 'model_apply_output/build_route_with_seq_and_package.csv')
        generate_zone_att_apply(data, save_file, data_apply_output_path)

