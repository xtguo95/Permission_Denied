import json
import time
import pandas as pd
import numpy as np

def zone_based_dist_compute(route_data_df, travel_time_matrix, save_file, mode,
                            data_output_path, data_apply_output_path):




    route_list = route_data_df.route_id.unique().tolist()

    zone_based_mean_distance_matrix = {}
    zone_based_median_distance_matrix = {}
    zone_based_max_distance_matrix = {}
    zone_based_min_distance_matrix = {}
    zone_based_std_distance_matrix = {}

    for route_id in route_list:
        distance_matrix = travel_time_matrix[route_id]
        route_df = route_data_df[route_data_df.route_id == route_id]

        zone_list = route_df.zone_id.unique()

        zone_stop_dict = {}
        for i in zone_list:
            zone_stops = list(route_df[route_df.zone_id == i].stops.values)
            zone_stop_dict[i] = zone_stops

        mean_matrix = {}
        median_matrix = {}
        min_matrix = {}
        max_matrix = {}
        std_matrix = {}

        zone_based_mean_distance_matrix[route_id] = mean_matrix
        zone_based_median_distance_matrix[route_id] = median_matrix
        zone_based_max_distance_matrix[route_id] = max_matrix
        zone_based_min_distance_matrix[route_id] = min_matrix
        zone_based_std_distance_matrix[route_id] = std_matrix

        # Initialization
        for i in zone_list:
            mean_matrix[i] = {}
            median_matrix[i] = {}
            min_matrix[i] = {}
            max_matrix[i] = {}
            std_matrix[i] = {}
            for j in zone_list:
                mean_matrix[i][j] = 0
                median_matrix[i][j] = 0
                min_matrix[i][j] = 0
                max_matrix[i][j] = 0
                std_matrix[i][j] = 0
        for i in zone_list:
            zone_stop_list_i = zone_stop_dict[i]
            for j in zone_list:
                zone_stop_list_j = zone_stop_dict[j]
                generalized_dist_list = []
                for stop_i in zone_stop_list_i:
                    for stop_j in zone_stop_list_j:
                        dist = distance_matrix[stop_i][stop_j]
                        generalized_dist_list.append(dist)

                mean_matrix[i][j] = np.mean(generalized_dist_list)
                median_matrix[i][j] = np.median(generalized_dist_list)
                min_matrix[i][j] = np.min(generalized_dist_list)
                max_matrix[i][j] = np.max(generalized_dist_list)
                std_matrix[i][j] = np.std(generalized_dist_list)


    if save_file:
        if mode == 'build':
            with open(data_output_path + 'data/zone_mean_travel_times.json', 'w') as json_file:
                json.dump(zone_based_mean_distance_matrix, json_file)
            with open(data_output_path + 'data/zone_min_travel_times.json', 'w') as json_file:
                json.dump(zone_based_min_distance_matrix, json_file)
        elif mode == 'apply':
            with open(data_apply_output_path + 'model_apply_output/zone_mean_travel_times.json', 'w') as json_file:
                json.dump(zone_based_mean_distance_matrix, json_file)
            with open(data_apply_output_path + 'model_apply_output/zone_min_travel_times.json', 'w') as json_file:
                json.dump(zone_based_min_distance_matrix, json_file)
        #
        # with open('zone_median_travel_times.json', 'w') as json_file:
        #     json.dump(zone_based_median_distance_matrix, json_file)
        #
        # with open('zone_max_travel_times.json', 'w') as json_file:
        #     json.dump(zone_based_max_distance_matrix, json_file)

        # with open('zone_std_travel_times.json', 'w') as json_file:
        #     json.dump(zone_based_std_distance_matrix, json_file)

    return zone_based_mean_distance_matrix, zone_based_min_distance_matrix



def main(data_path, mode):
    tic = time.time()

    if mode == 'build':
        data_output_path = data_path + 'model_build_outputs/'
        data_apply_output_path = None
        filepath = data_path + 'model_build_inputs/travel_times.json'
        with open(filepath, newline='') as in_file:
            travel_time_matrix = json.load(in_file)
        route_data_df = pd.read_csv(data_output_path + 'data/build_route_with_seq.csv', na_values='',
                                    keep_default_na=False)

    elif mode == 'apply':
        data_output_path = data_path + 'model_build_outputs/'
        data_apply_output_path = data_path + 'model_apply_outputs/'
        filepath = data_path + 'model_apply_inputs/new_travel_times.json'
        with open(filepath, newline='') as in_file:
            travel_time_matrix = json.load(in_file)
        route_data_df = pd.read_csv(data_apply_output_path + 'model_apply_output/build_route_with_seq.csv', na_values='',
                                    keep_default_na=False)


    save_file = True
    zone_based_mean_distance_matrix, zone_based_min_distance_matrix = zone_based_dist_compute(route_data_df,
                                                                                              travel_time_matrix,
                                                                                              save_file, mode,
                                                                                              data_output_path, data_apply_output_path)
    print('generate zone dist time', time.time() - tic)

if __name__ == '__main__':
    tic = time.time()

    mode = 'build'

    if mode == 'build':
        data_output_path = '../data_fake/model_build_outputs/'
        data_apply_output_path = None
        filepath = '../data/model_build_inputs/travel_times.json'
        with open(filepath, newline='') as in_file:
            travel_time_matrix = json.load(in_file)
        route_data_df = pd.read_csv(data_output_path + 'data/build_route_with_seq.csv', na_values='',
                                    keep_default_na=False)

    elif mode == 'apply':
        data_output_path = '../data_fake/model_build_outputs/'
        data_apply_output_path = '../data_fake/model_apply_outputs/'
        filepath = '../data/model_apply_inputs/new_travel_times.json'
        with open(filepath, newline='') as in_file:
            travel_time_matrix = json.load(in_file)
        route_data_df = pd.read_csv(data_apply_output_path + 'model_apply_output/build_route_with_seq.csv', na_values='',
                                    keep_default_na=False)




    save_file = True
    zone_based_mean_distance_matrix, zone_based_min_distance_matrix = zone_based_dist_compute(route_data_df,
                                                                                              travel_time_matrix,
                                                                                              save_file, mode,
                                                                                              data_output_path, data_apply_output_path)
    print('generate zone dist time', time.time() - tic)
