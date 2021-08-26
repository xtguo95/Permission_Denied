from os import path
import sys, json, time
import pandas as pd


def generate_package_data(data_path, save_file, mode,  data_output_path, data_apply_output_path):
    # Read input data
    print('Reading Input Data for generate_package_data')
    if mode == 'build':
        with open(data_path + 'model_'+mode+'_inputs/package_data.json', newline='') as in_file:
            package_data = json.load(in_file)

        package_df = {'route_id':[],'stops':[],'pack_ID':[],'scan_status':[],'time_window_start':[],'time_window_end':[],'planned_service_time':[],'depth_cm':[],'height_cm':[],'width_cm':[]}

        for route_id in package_data:
            route = package_data[route_id]
            for stops in route:
                for pack in route[stops]:
                    package_df['route_id'].append(route_id)
                    package_df['stops'].append(stops)
                    package_df['pack_ID'].append(pack)
                    package_df['scan_status'].append(route[stops][pack]['scan_status'])
                    package_df['time_window_start'].append(route[stops][pack]['time_window']['start_time_utc'])
                    package_df['time_window_end'].append(route[stops][pack]['time_window']['end_time_utc'])
                    package_df['planned_service_time'].append(route[stops][pack]['planned_service_time_seconds'])
                    package_df['depth_cm'].append(route[stops][pack]['dimensions']['depth_cm'])
                    package_df['height_cm'].append(route[stops][pack]['dimensions']['height_cm'])
                    package_df['width_cm'].append(route[stops][pack]['dimensions']['width_cm'])

        package_df = pd.DataFrame(package_df)
        package_df['stops'] = package_df['stops'].astype(str)
        if save_file:
            package_df.to_csv(data_output_path + 'data/'+mode+'_package_df.csv',index=False)

    elif mode == 'apply':
        with open(data_path + 'model_'+mode+'_inputs/new_package_data.json', newline='') as in_file:
            package_data = json.load(in_file)

        package_df = {'route_id':[],'stops':[],'pack_ID':[],'time_window_start':[],'time_window_end':[],'planned_service_time':[],'depth_cm':[],'height_cm':[],'width_cm':[]}

        for route_id in package_data:
            route = package_data[route_id]
            for stops in route:
                for pack in route[stops]:
                    package_df['route_id'].append(route_id)
                    package_df['stops'].append(stops)
                    package_df['pack_ID'].append(pack)
                    package_df['time_window_start'].append(route[stops][pack]['time_window']['start_time_utc'])
                    package_df['time_window_end'].append(route[stops][pack]['time_window']['end_time_utc'])
                    package_df['planned_service_time'].append(route[stops][pack]['planned_service_time_seconds'])
                    package_df['depth_cm'].append(route[stops][pack]['dimensions']['depth_cm'])
                    package_df['height_cm'].append(route[stops][pack]['dimensions']['height_cm'])
                    package_df['width_cm'].append(route[stops][pack]['dimensions']['width_cm'])

        package_df = pd.DataFrame(package_df)

        package_df['stops'] = package_df['stops'].astype(str)


        if save_file:
            package_df.to_csv(data_apply_output_path + 'model_apply_output/'+mode+'_package_df.csv',index=False)

    return package_df


if __name__ == '__main__':
    data_path = '../data/'
    save_file = False
    mode = 'build'
    data_output_path = ''
    data_apply_output_path = ''
    generate_package_data(data_path, save_file, mode, data_output_path, data_apply_output_path)

   
