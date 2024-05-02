from calculate_avg_and_sum import AvgAndSum
from calculate_class_probability import ClassProbability
from calculate_expected_value import ExpectedValue
from calculate_observed_value import ObservedValue
from calculate_chi2 import ChiSquare
from calculate_distance_between_centroids import DisatanceBetweenCentroids
from remove_centroid import RemoveCentroid
from select_highest import SelectHighest
import pandas as pd

# Define the file paths
file_A = "./selectKBest-chi2/output/chi2.txt"
file_B = "./input/user_item_matrix copy.txt"

# Define needed var
k = 3  # Number of highest values to select
noItem = '7'
a = 1

def run_mr_job(mr_job_class, input_args):
    mr_job = mr_job_class(args=input_args)
    with mr_job.make_runner() as runner:
        runner.run()
        data = []
        for key, value in mr_job.parse_output(runner.cat_output()):
            data.append(f"{key}\t{value}")
        return data


def write_data_to_file(filename, data, mode="w"):
    output_file = open(filename, mode)
    for el in data:
        output_file.writelines(el)
    output_file.close()

result_data = run_mr_job(AvgAndSum, ["./input/input_file_copy.txt", "--n", noItem])
write_data_to_file("./selectKBest-chi2/output/avg_sum.txt", result_data)

result_data = run_mr_job(ClassProbability, ["./input/items copy.txt", "--n", noItem])
write_data_to_file("./selectKBest-chi2/output/class_probability.txt", result_data)

result_data = run_mr_job(
    ObservedValue,
    ["./input/items copy.txt", "./input/user_item_matrix copy.txt"],
)
write_data_to_file("./selectKBest-chi2/output/O.txt", result_data)

result_data = run_mr_job(
    ExpectedValue,
    [
        "./selectKBest-chi2/output/avg_sum.txt",
        "--cProb",
        "./selectKBest-chi2/output/class_probability.txt",
    ],
)
write_data_to_file("./selectKBest-chi2/output/E.txt", result_data)

result_data = run_mr_job(
    ChiSquare,
    [
        "./selectKBest-chi2/output/E.txt",
        "./selectKBest-chi2/output/O.txt",
    ],
)
write_data_to_file("./selectKBest-chi2/output/chi2.txt", result_data)



# Read data from file A into a pandas DataFrame
data_A_df = pd.read_csv(file_A, sep="\t", header=None, names=["key", "value"])

# Read data from file B into a pandas DataFrame
data_B_df = pd.read_csv(file_B, sep="\t", header=None, names=["key", "value"])

# Convert 'key' column to integer type
data_B_df["key"] = data_B_df["key"].astype(int)

# Merge data_A_df and data_B_df on 'key'
merged_df = pd.merge(data_A_df, data_B_df, on="key", how="inner")

# Sort merged DataFrame based on 'value_x' and select the top k elements
sorted_merged_df = merged_df.sort_values(by="value_x", ascending=False).head(k)

# Retrieve corresponding values from sorted_merged_df
corresponding_values = sorted_merged_df[["key", "value_y"]].values.tolist()

# Write new centroids to file
curr_user = str(corresponding_values[0][0])
curr_coor = corresponding_values[0][1]

with open('selectKBest-chi2/output/centroids.txt','w') as file:
    for i in corresponding_values:
        file.writelines(f"{i[0]}\t{i[1]}\n")
        i[1] = [float(coor.strip().split(';')[1] )for coor in (i[1].strip().split('|'))]

for i in range(2):

    result_data = run_mr_job(
        RemoveCentroid,
        [
            "./selectKBest-chi2/output/centroids.txt",
            "--centroid",
            curr_user
        ]
    )
    write_data_to_file(f"./selectKBest-chi2/output/centroids.txt",result_data)
    
    result_data = run_mr_job(
        DisatanceBetweenCentroids,
        [
            "./selectKBest-chi2/output/centroids.txt",
            "--centroid-coord",
            curr_coor
        ]
    )
    write_data_to_file(f"./selectKBest-chi2/output/distance.txt",result_data)
    
    result_data = run_mr_job(
        SelectHighest,
        [
            "./selectKBest-chi2/output/distance.txt",
        ]
    )

    write_data_to_file(f"./selectKBest-chi2/output/top.txt", result_data)

    with open("./selectKBest-chi2/output/top.txt", "r") as file:
        for line in file:
            curr_user = line.strip().split('\t')[0]

    a = 0
    corresponding_values = 0