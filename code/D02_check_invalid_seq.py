import json
import pandas as pd
import random
import numpy as np


def isinvalid(actual, sub):
    if len(actual) != len(sub) or set(actual) != set(sub):
        return True
    elif actual[0] != sub[0]:
        return True
    else:
        return False

def generate_random_seq(actual):
    return actual

# def delete_some_ele(seq):
#     idx_to_preserve = np.random.choice(range(len(seq)), size = int(np.round(len(seq)*0.7)), replace = False)
#     new_seq = [seq[int(key)] for key in idx_to_preserve]
#     return new_seq
#
# def add_some_ele(seq):
#     idx_to_dup = np.random.choice(range(len(seq)), size = int(np.round(len(seq)*0.2)), replace = False)
#     new_seq_add= [seq[int(key)] for key in idx_to_dup]
#     new_seq = new_seq_add + seq
#     return new_seq
#
# def shuffle_seq(seq):
#     np.random.shuffle(seq)
#     return seq


def main(data_path):
    with open(data_path + "model_apply_outputs/model_apply_output/mean_dist/opt_complete_seq_tour_post_process.json") as f:
        complete_seq_dict = json.load(f)

    whole_seq = pd.read_csv(data_path + "model_apply_outputs/model_apply_output/build_route_with_seq.csv")
    whole_seq['stops'] = whole_seq['stops'].fillna('NA')
    whole_seq['seq_temp'] = 100
    whole_seq.loc[whole_seq['zone_id'] == 'INIT','seq_temp'] = 1
    whole_seq = whole_seq.sort_values(['route_id','seq_temp','zone_id'])

    whole_seq_dict = whole_seq.groupby(['route_id']).apply(lambda x: list(x['stops'])).to_dict()

    count = 0
    new_seq_dict = {}
    for key in complete_seq_dict:
        actual_seq_only_first = whole_seq_dict[key]
        sub_seq = complete_seq_dict[key]

        if isinvalid(actual_seq_only_first, sub_seq):
            count += 1
            inters_ele = [ele for ele in actual_seq_only_first if ele in sub_seq]#set(actual_seq_only_first).intersection(set(sub_seq))
            remain_ele = [ele for ele in actual_seq_only_first if ele not in sub_seq] # keep sequence
            new_sub_seq = []
            for key in sub_seq:
                if key in inters_ele and key not in new_sub_seq:
                    new_sub_seq.append(key)
            new_sub_seq += list(remain_ele)

            # check first
            if new_sub_seq[0] != actual_seq_only_first[0]:
                if actual_seq_only_first[0] in new_sub_seq:
                    new_sub_seq.remove(actual_seq_only_first[0])
                    new_sub_seq = [actual_seq_only_first[0]] + new_sub_seq
                else:
                    # should not run this, just for conservation
                    print("should not 1")
                    new_sub_seq = generate_random_seq(actual_seq_only_first)
            else:
                if isinvalid(actual_seq_only_first, new_sub_seq):
                    # should not run this, just for conservation
                    print("should not 2")
                    new_sub_seq = generate_random_seq(actual_seq_only_first)
            new_seq_dict[key] = new_sub_seq
        else:
            new_seq_dict[key] = complete_seq_dict[key]


    print('invalid seq',count)

if __name__ == '__main__':
    data_path = '../data_fake/'
    main(data_path)
    # a=1


