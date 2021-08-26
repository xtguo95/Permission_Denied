import A04_merge_all_data
import A05_get_zone_attributes
import A06_zone_based_dist_compute
import A07_zone_tt

# ####step 1
print('=======Start step 1, A04')
mode = 'build'
data_path = '../data_fake/'
A04_merge_all_data.main(data_path, mode)
print('=======Finish build step 1, A04')
#

####step 2
print('=======Start step 2, A05')
mode = 'build'
data_path = '../data_fake/'
A05_get_zone_attributes.main(data_path, mode)
print('=======Finish build step 2, A05')


#step 3
print('=======Start step 3, A06')
mode = 'build'
data_path = '../data_fake/'
A06_zone_based_dist_compute.main(data_path, mode)
print('=======Finish build step 3, A06')


#step 4
print('=======Start step 4, A07')
mode = 'build'
data_path = '../data_fake/'
near_num_neighbor_out = 9
further_num_neighbor_out = 9
A07_zone_tt.main(data_path, mode, near_num_neighbor_out, further_num_neighbor_out)
print('=======Finish build step 4, A07')






