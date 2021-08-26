# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import pickle as pkl
import sys
import json
import A04_merge_all_data
import A05_get_zone_attributes
import A06_zone_based_dist_compute
import time
import os

def preprocess_all(data_path, data_output_path, save_file, data_apply_output_path, mode):

    tic = time.time()
    route_seq_fill_na, route_seq_package = A04_merge_all_data.get_all_data(data_path, read_file = False, save_file = save_file,
                                                                           previous_save_file = True, mode = mode,
                                                                           data_output_path = data_output_path,
                                                                           data_apply_output_path = data_apply_output_path)
    zone_data = A05_get_zone_attributes.generate_zone_att_apply(route_seq_package, save_file = save_file,
                                                                data_apply_output_path = data_apply_output_path)

    print('finish generate zone data', time.time() - tic)



def generate_zone_travel_time(data_path, mode):
    tic = time.time()
    if mode == 'apply':
        data_output_path = data_path + 'model_build_outputs/'
        data_apply_output_path = data_path + 'model_apply_outputs/'
        route_seq_fill_na = pd.read_csv(data_apply_output_path + 'model_apply_output/build_route_with_seq.csv', na_values='',
                                    keep_default_na=False)
    elif mode == 'build':
        data_output_path = data_path + 'model_build_outputs/'
        data_apply_output_path = None
        route_seq_fill_na = pd.read_csv(data_apply_output_path + 'data/build_route_with_seq.csv', na_values='',
                                    keep_default_na=False)
    ################
    filepath = data_path + 'model_' + mode + '_inputs/new_travel_times.json'
    with open(filepath, newline='') as in_file:
        travel_time_matrix = json.load(in_file)
    tt_avg, tt_min = A06_zone_based_dist_compute.zone_based_dist_compute(route_seq_fill_na, travel_time_matrix, save_file = True, mode = mode,
                                                                         data_output_path = data_output_path,
                                                                         data_apply_output_path = data_apply_output_path)
    ##################
    print('finish generate zone distance', time.time() - tic)



def process_apply_data(data_path, save_file,  data_output_path, data_apply_output_path, mode):

    preprocess_all(data_path, data_output_path, save_file, data_apply_output_path, mode)

def main(data_path, mode):
    data_output_path = data_path + 'model_build_outputs/'
    data_apply_output_path = data_path + 'model_apply_outputs/'

    data_apply_output_path_save = data_apply_output_path + 'model_apply_output/'
    if not os.path.exists(data_apply_output_path_save):
        os.mkdir(data_apply_output_path_save)

    mean_dist_path = data_apply_output_path + 'model_apply_output/mean_dist/'
    if not os.path.exists(mean_dist_path):
        os.mkdir(mean_dist_path)

    process_apply_data(data_path, save_file = True,
                       data_output_path = data_output_path,
                       data_apply_output_path = data_apply_output_path, mode = mode)

if __name__ == '__main__':
    data_path =  '../data_fake/'
    data_output_path = '../data_fake/model_build_outputs/'
    data_apply_output_path = '../data_fake/model_apply_outputs/'

    data_apply_output_path_save = data_apply_output_path + 'model_apply_output/'
    if not os.path.exists(data_apply_output_path_save):
        os.mkdir(data_apply_output_path_save)


    process_apply_data(data_path, save_file = True,
                       data_output_path = data_output_path,
                       data_apply_output_path = data_apply_output_path, mode = 'apply')