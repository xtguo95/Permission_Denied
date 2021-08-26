import pandas as pd
import json
import numpy as np
import copy
import pickle


def post_process_with_service_time(data_path, raw_seq):

    if raw_seq:
        with open(data_path + "model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour.json") as f:
            complete_seq_dict = json.load(f)
    else:
        with open(data_path + "model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour_post_process.json") as f:
            complete_seq_dict = json.load(f)


    route_opt_seq_package = pd.read_csv(data_path + "model_apply_outputs/model_apply_output/build_route_with_seq_and_package.csv", na_values='',
                                        keep_default_na=False)


    route_opt_seq = route_opt_seq_package.groupby(['route_id', 'zone_id', 'stops'], as_index=False).mean()[
        ['route_id', 'zone_id', 'stops', 'planned_service_time']]
    route_opt_seq['planned_service_time'] = route_opt_seq['planned_service_time'].fillna(0)


    prop = 0.15


    df_dict = {'route_id':[],'stops':[],'pred_seq_id':[]}
    for key in complete_seq_dict:
        pred_seq_id = 1
        for stop_id in complete_seq_dict[key]:
            df_dict['route_id'].append(key)
            df_dict['stops'].append(stop_id)
            df_dict['pred_seq_id'].append(pred_seq_id)
            pred_seq_id += 1
    tsp_df = pd.DataFrame(df_dict)

    tsp_df_with_s_t = tsp_df.merge(route_opt_seq, on = ['route_id','stops'])

    tsp_df_with_s_t['i'] = tsp_df_with_s_t.groupby('route_id', as_index=False).cumcount()
    tsp_df_with_s_t['num_stops'] = tsp_df_with_s_t.groupby('route_id', as_index=False)['stops'].transform("count")
    tsp_df_with_s_t['prop'] = tsp_df_with_s_t['i'] / tsp_df_with_s_t['num_stops']

    first_zones = \
        tsp_df_with_s_t[tsp_df_with_s_t['prop'] <= prop][['route_id', 'planned_service_time']].groupby('route_id',
                                                                                         as_index=False).sum()[
            ['route_id', 'planned_service_time']]

    last_zones = tsp_df_with_s_t[tsp_df_with_s_t['prop'] > 1 - prop][['route_id', 'planned_service_time']].groupby('route_id',
                                                                                                     as_index=False).sum()[
        ['route_id', 'planned_service_time']]

    first_zones = first_zones.rename(columns = {'planned_service_time':'first15_servicetime'})
    last_zones = last_zones.rename(columns = {'planned_service_time':'last15_servicetime'})
    df = first_zones.merge(last_zones, on = ['route_id'])

    factor = 1.22
    df['forward'] = df['first15_servicetime'] * factor > df['last15_servicetime']
    df['forward'] = df['forward'].astype('int')


    df['forward'] = df['first15_servicetime'] * factor > df['last15_servicetime']

    #
    # score = pd.read_csv("score_reverse.csv")
    # score_test = pd.merge(score, df, on='route_id')
    # print(1223 - score_test['forward'].sum())
    # print(factor, np.mean(
    #     score_test['forward'] * score_test['score'] + (1 - score_test['forward']) * score_test['score_rev']))



    need_to_change_routes = list(df.loc[df['forward'] == 0,'route_id'])


    with open(
            data_path + "model_apply_outputs/model_apply_output/mean_dist/service_time_changed_routes.pickle", 'wb') as f:
        pickle.dump(need_to_change_routes, f)

    # print(len(need_to_change_routes))

    new_seq_dict = {}

    for key in complete_seq_dict:
        if key in need_to_change_routes:
            seq = complete_seq_dict[key]
            sub_seq = copy.deepcopy(seq[1:])
            sub_seq.reverse()
            new_seq = [seq[0]] + sub_seq
            new_seq_dict[key] = new_seq
        else:
            new_seq_dict[key] = complete_seq_dict[key]



    with open(data_path + "model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour_post_process.json", 'w') as f:
        json.dump(new_seq_dict, f)


def post_process_with_pkg_volume(data_path, raw_seq):

    if raw_seq:
        with open(data_path + "model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour.json") as f:
            complete_seq_dict = json.load(f)
    else:
        with open(data_path + "model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour.json") as f:
            complete_seq_dict = json.load(f)

        with open(
                data_path + "model_apply_outputs/model_apply_output/mean_dist/service_time_changed_routes.pickle", 'rb') as f:
            service_time_changed_routes = pickle.load(f)


    route_opt_seq_package = pd.read_csv(data_path + "model_apply_outputs/model_apply_output/build_route_with_seq_and_package.csv", na_values='',
                                        keep_default_na=False)

    route_opt_seq_package['volume'] = route_opt_seq_package['depth_cm'] * route_opt_seq_package['height_cm'] * route_opt_seq_package['width_cm']


    route_opt_seq = route_opt_seq_package.groupby(['route_id', 'zone_id', 'stops'], as_index=False)['volume'].sum()

    prop = 0.15
    factor = 3

    df_dict = {'route_id': [], 'stops': [], 'pred_seq_id': []}
    for key in complete_seq_dict:
        pred_seq_id = 1
        for stop_id in complete_seq_dict[key]:
            df_dict['route_id'].append(key)
            df_dict['stops'].append(stop_id)
            df_dict['pred_seq_id'].append(pred_seq_id)
            pred_seq_id += 1

    tsp_df = pd.DataFrame(df_dict)

    tsp_df_with_s_t = tsp_df.merge(route_opt_seq, on=['route_id', 'stops'])

    tsp_df_with_s_t['i'] = tsp_df_with_s_t.groupby('route_id', as_index=False).cumcount()
    tsp_df_with_s_t['num_stops'] = tsp_df_with_s_t.groupby('route_id', as_index=False)['stops'].transform("count")
    tsp_df_with_s_t['prop'] = tsp_df_with_s_t['i'] / tsp_df_with_s_t['num_stops']

    first_zones = tsp_df_with_s_t[tsp_df_with_s_t['prop'] <= prop][['route_id', 'volume']].groupby('route_id',
                                                                                                   as_index=False).sum()[
        ['route_id', 'volume']]
    #r = 'RouteID_7546109b-42da-44d3-9af7-1772fcfa8a3b'
    #test = last_zones.loc[first_zones['route_id'] == r]
    last_zones = tsp_df_with_s_t[tsp_df_with_s_t['prop'] > 1 - prop][['route_id', 'volume']].groupby('route_id',
                                                                                                     as_index=False).sum()[['route_id', 'volume']]

    first_zones = first_zones.rename(columns={'volume': 'first15_volume'})
    last_zones = last_zones.rename(columns={'volume': 'last15_volume'})
    df = first_zones.merge(last_zones, on=['route_id'])

    df['first15_volume'] = df['first15_volume'].astype(float)
    df['last15_volume'] = df['last15_volume'].astype(float)

    #
    df['forward'] = df['first15_volume'] * factor > df['last15_volume']

    need_to_change_routes = list(df.loc[df['forward'] == 0,'route_id'])

    with open(
            data_path + "model_apply_outputs/model_apply_output/mean_dist/pkg_volume_changed_routes.pickle", 'wb') as f:
        pickle.dump(need_to_change_routes, f)


    if not raw_seq:
        need_to_change_routes_new = list(set(need_to_change_routes).difference(set(service_time_changed_routes)))
        not_change_routes = list(set(need_to_change_routes).difference(set(need_to_change_routes_new)))
        df.loc[df['route_id'].isin(not_change_routes), 'forward'] = 1
    else:
        need_to_change_routes_new = need_to_change_routes
        #for key in need_to_change_routes:


    print('num changed routes pkg volume', len(need_to_change_routes_new))

    # score = pd.read_csv("score_reverse.csv")
    # score_test = pd.merge(score, df, on='route_id')
    # print(1223 - score_test['forward'].sum())
    # print(factor, np.mean(
    # score_test['forward'] * score_test['score'] + (1 - score_test['forward']) * score_test['score_rev']))

    with open(data_path + "model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour_post_process.json") as f:
        complete_seq_dict_post_process = json.load(f)

    new_seq_dict = {}

    for key in complete_seq_dict_post_process:
        if key in need_to_change_routes_new:
            seq = complete_seq_dict_post_process[key]
            sub_seq = copy.deepcopy(seq[1:])
            sub_seq.reverse()
            new_seq = [seq[0]] + sub_seq
            new_seq_dict[key] = new_seq
        else:
            new_seq_dict[key] = complete_seq_dict_post_process[key]


    with open(data_path + "model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour_post_process.json", 'w') as f:
        json.dump(new_seq_dict, f)

def main(data_path):

    post_process_with_service_time(data_path, raw_seq= True)
    post_process_with_pkg_volume(data_path, raw_seq= False)


#
#     #a=1
#     #
#     # #for i in range(10):
#     #
#     #
#     # score = pd.read_csv("score_reverse.csv")
#     # score['diff'] = score['score_rev'] - score['score']
#     # score = score.sort_values('diff')
#     #
#     # #
#     # for factor in np.arange(1.1, 1.3, 0.01):
#     #     test_results = []
#     #     for i in range(30):
#     #         score_ = score.sample(frac=0.5)
#     #
#     #         df['forward'] = df['first15_servicetime'] * factor > df['last15_servicetime']
#     #         score_test = pd.merge(score_, df, on='route_id')
#     #
#     #         #print(1223 - score_test['forward'].sum())
#     #         score_new = np.mean(score_test['forward'] * score_test['score'] + (1 - score_test['forward']) * score_test['score_rev'])
#     #         score_old = np.mean(score_test['score'])
#     #         diff = score_old - score_new
#     #         test_results.append((factor, score_new, score_old, diff))
#     #         # print(factor,
#     #         #       )
#     #
#     #     test_results.sort(key=lambda x:x[3])
#     #     print('worst test ',test_results[0])
#     #     a=1

if __name__ == '__main__':
    data_path = '../data_fake/'
    main(data_path)