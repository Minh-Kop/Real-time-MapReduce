import pandas as pd

# input_file path
origin_file_path = "./input/input_file_1M.txt"

# item_file path
item_file_path = "./input/items_1M.txt"

# output path
input_train_path = "./input/input_file_1M_train.txt"
input_test_path = "./input/input_file_1M_test.txt"
item_train_path = "./input/items_1M_train.txt"
item_test_path = "./input/items_1M_test.txt"

# read input_file and item_file
origin_rating_df = pd.read_csv(origin_file_path, names=["user_item_rating_time"])
item_df = pd.read_csv(
    item_file_path, sep="\t", names=["item", "categories"], dtype="str"
)

# split input_file to train and test
input_file_train_df = origin_rating_df.sample(frac=0.8)
input_file_test_df = origin_rating_df.drop(input_file_train_df.index)

# split train_items_file
item_train_df = input_file_train_df.copy()
item_train_df[["user_item", "rating_time"]] = item_train_df[
    "user_item_rating_time"
].str.split("\t", expand=True)

item_train_df[["user", "item"]] = item_train_df["user_item"].str.split(";", expand=True)
item_file_train_df = pd.DataFrame(
    item_train_df["item"].unique(), columns=["item"]
).merge(item_df, on="item")

# split test_item_file
item_test_df = input_file_test_df.copy()
item_test_df[["user_item", "rating_time"]] = item_test_df[
    "user_item_rating_time"
].str.split("\t", expand=True)

item_test_df[["user", "item"]] = item_test_df["user_item"].str.split(";", expand=True)
item_file_test_df = pd.DataFrame(item_test_df["item"].unique(), columns=["item"]).merge(
    item_df, on="item"
)

# save train_input and test_input
input_file_train_df.to_csv(input_train_path, index=False, header=False)
input_file_test_df.to_csv(input_test_path, index=False, header=False)

# save train_item and test_item
item_file_train_df.to_csv(item_train_path, index=False, header=False)
item_file_test_df.to_csv(item_test_path, index=False, header=False)
