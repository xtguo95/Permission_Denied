from os import path
import sys, json, time
import pandas as pd

# Read input data

def generate_build_route_seq_df(data_path, save_file, data_output_path):
    file_path = data_path + 'model_build_inputs/actual_sequences.json'
    print('Reading Input Data for generate_build_route_seq_df')
    with open(file_path, newline='') as in_file:
        actual_sequences = json.load(in_file)

    seq_df = {'route_id':[],'stops':[],'seq_ID':[]}

    for route_id in actual_sequences:
        route = actual_sequences[route_id]
        for stops in route['actual']:
            seq_df['route_id'].append(route_id)
            if stops == '':
                print('empty')
                break
            seq_df['stops'].append(stops)
            seq_df['seq_ID'].append(route['actual'][stops])


    seq_df = pd.DataFrame(seq_df)
    seq_df['stops'] = seq_df['stops'].astype(str)

    if save_file:
        seq_df.to_csv(data_output_path + 'data/build_route_seq_df.csv',index=False)

    # #%%
    # with open('../../data/model_build_inputs/invalid_sequence_scores.json', newline='') as in_file:
    #     invalid = json.load(in_file)
    #
    # invalid = pd.DataFrame.from_dict(invalid, orient='index', columns=['invalid_score']).reset_index()
    # invalid.columns = ['route_id','invalid_score']
    # invalid.to_csv("../data/build_invalid_score_df.csv", index=False)
    #

    return seq_df

if __name__ == '__main__':
    data_path = '../data_fake/'
    save_file = False
    data_output_path = ''
    generate_build_route_seq_df(data_path, save_file, data_output_path)