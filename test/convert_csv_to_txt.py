import os
import pandas as pd


def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)


# Đọc dữ liệu từ file CSV
df = pd.read_csv(create_path('u.csv'), sep=';')
print(df.iloc[[0]])

# # Thay đổi dấu ';' thứ hai thành dấu tab ('\t')
# df['item_id'] = df['item_id'].apply(lambda x: x.replace(';', '\t'))
# 
# # Lưu dữ liệu vào file u.txt với dấu tab
# df.to_csv(create_path('u.txt'), sep='\t', index=False)
