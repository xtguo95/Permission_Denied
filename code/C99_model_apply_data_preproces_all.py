# -*- coding: utf-8 -*-

import C01_preprocess_model_apply
import A07_zone_tt
from os import path

BASE_DIR = path.dirname(path.dirname(path.abspath(__file__)))
data_path = path.join(BASE_DIR,'data/')

#step 1
print('=======Start process model apply')
mode = 'apply'
C01_preprocess_model_apply.main(data_path, mode)
print('=======Finish process data')

print('=======Start process zone based travel time apply')
mode = 'apply'
C01_preprocess_model_apply.generate_zone_travel_time(data_path, mode)
print('=======Finish process data')

# #step 4
print('=======Start step 2, A07')
mode = 'apply'
near_num_neighbor_out = 9
further_num_neighbor_out = 9
A07_zone_tt.main(data_path, mode, near_num_neighbor_out, further_num_neighbor_out)
print('=======Finish build step 2, A07')
