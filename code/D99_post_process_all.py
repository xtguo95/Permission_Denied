import D01_post_process_pred_seq
import D02_check_invalid_seq
from os import path

BASE_DIR = path.dirname(path.dirname(path.abspath(__file__)))
data_path = path.join(BASE_DIR,'data/')
#step 1
print('=======Start post process D01')
D01_post_process_pred_seq.main(data_path)
print('=======Finish D01')

print('=======Start post process D02')
D02_check_invalid_seq.main(data_path)
print('=======Finish process data')
