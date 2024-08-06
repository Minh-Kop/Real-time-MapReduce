from calculate_avg_and_sum import AvgAndSum
from calculate_class_probability import ClassProbability
from calculate_expected_value import ExpectedValue
from calculate_observed_value import ObservedValue
from calculate_chi2 import ChiSquare
from calculate_distance_between_centroids import DistanceBetweenCentroids
from remove_centroid import RemoveCentroid
from get_max import GetMax
from get_current_centroid import CurrentCentroid
import pandas as pd

# Define the file paths
file_A = "./clustering/proposal_2_chi2/output/chi2.txt"
file_B = "./input/user_item_matrix copy.txt"

# Define needed var
k = 3  # Number of highest values to select
noItem = "7"
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
write_data_to_file("./clustering/proposal_2_chi2/output/avg_sum.txt", result_data)

result_data = run_mr_job(ClassProbability, ["./input/items_copy.txt", "--n", noItem])
write_data_to_file(
    "./clustering/proposal_2_chi2/output/class_probability.txt", result_data
)

result_data = run_mr_job(
    ObservedValue,
    ["./input/items_copy.txt", "./input/user_item_matrix copy.txt"],
)
write_data_to_file("./clustering/proposal_2_chi2/output/O.txt", result_data)

result_data = run_mr_job(
    ExpectedValue,
    [
        "./clustering/proposal_2_chi2/output/avg_sum.txt",
        "--categories-probability-path",
        "./clustering/proposal_2_chi2/output/class_probability.txt",
    ],
)
write_data_to_file("./clustering/proposal_2_chi2/output/E.txt", result_data)

result_data = run_mr_job(
    ChiSquare,
    [
        "./clustering/proposal_2_chi2/output/E.txt",
        "./clustering/proposal_2_chi2/output/O.txt",
    ],
)
write_data_to_file("./clustering/proposal_2_chi2/output/chi2.txt", result_data)

# Read data from file A into a pandas DataFrame
data_A_df = pd.read_csv(file_A, sep="\t", header=None, names=["key", "value"])

# Read data from file B into a pandas DataFrame
data_B_df = pd.read_csv(file_B, sep="\t", header=None, names=["key", "value"])

# Merge data_A_df and data_B_df on 'key'
merged_df = pd.merge(data_A_df, data_B_df, on="key")

# Sort merged DataFrame based on 'value_x' and select the top k elements
sorted_merged_df = merged_df.sort_values(by="value_x", ascending=False).head(k * a)

# Retrieve corresponding values from sorted_merged_df
corresponding_values = sorted_merged_df[["key", "value_y"]].values.tolist()

# Write new centroids to file
curr_user = str(corresponding_values[0][0])
curr_coor = corresponding_values[0][1]

with open("./clustering/proposal_2_chi2/output/centroids.txt", "w") as file:
    for i in corresponding_values:
        file.writelines(f"{i[0]}\t{i[1]}\n")
        i[1] = [float(coor.strip().split(";")[1]) for coor in (i[1].strip().split("|"))]


# Remove current
result_data = run_mr_job(
    RemoveCentroid,
    ["./clustering/proposal_2_chi2/output/centroids.txt", "--centroid", curr_user],
)
write_data_to_file(f"./clustering/proposal_2_chi2/output/centroids.txt", result_data)

for i in range(2):
    print(f"Loop: {i + 1}")

    # Calculate distance between current to others centroids
    result_data = run_mr_job(
        DistanceBetweenCentroids,
        [
            "./clustering/proposal_2_chi2/output/centroids.txt",
            "--centroid-coord",
            curr_coor,
        ],
    )
    write_data_to_file(f"./clustering/proposal_2_chi2/output/distance.txt", result_data)

    # Get the highest centroid
    result_data = run_mr_job(
        GetMax,
        [
            "./clustering/proposal_2_chi2/output/distance.txt",
        ],
    )
    write_data_to_file(f"./clustering/proposal_2_chi2/output/top.txt", result_data)

    result_data = run_mr_job(
        CurrentCentroid,
        [
            "./clustering/proposal_2_chi2/output/top.txt",
            "./clustering/proposal_2_chi2/output/centroids.txt",
        ],
    )
    write_data_to_file(
        f"./clustering/proposal_2_chi2/output/current_centroid.txt", result_data
    )

    with open("./clustering/proposal_2_chi2/output/current_centroid.txt", "r") as file:
        for line in file:
            curr_user, curr_coor = line.strip().split("\t")

    # Remove current
    result_data = run_mr_job(
        RemoveCentroid,
        ["./clustering/proposal_2_chi2/output/centroids.txt", "--centroid", curr_user],
    )
    write_data_to_file(
        f"./clustering/proposal_2_chi2/output/centroids.txt", result_data
    )
