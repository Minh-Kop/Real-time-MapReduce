import pandas as pd
import numpy as np
import time

sTime = time.time()

# normalize data (convert user and item to reduce user/item that not exist in the table)
def to_id_coded_arr(raw_file_path, sep_char, r_cols):
    # bỏ qua dòng 1 - skiprows - là dòng tên cột
    data = pd.read_csv(raw_file_path, sep=sep_char, names=r_cols, encoding='latin-1', skiprows=1)

    data[r_cols[0]] = data[r_cols[0]].astype('category').cat.codes
    data[r_cols[1]] = data[r_cols[1]].astype('category').cat.codes
    
    data = data.to_numpy()

    data_rating = data[:, 0]
    data_rating = data_rating.astype(np.int32)
    return data

raw_file_path = './MovieLen100k.csv'
sep_char = ','
r_cols = ['user_id', 'item_id', 'rating']

data_coded = to_id_coded_arr(raw_file_path, sep_char, r_cols)
print(data_coded)