from calculate_avg_and_sum import AvgAndSum
from calculate_class_probability import ClassProbability
from calculate_expected_value import ExpectedValue
from calculate_observed_value import ObservedValue
from calculate_chi2 import ChiSquare
import pandas as pd

# Define the file paths
file_A = "./clustering/proposal_2_chi2/output/chi2.txt"
file_B = "./input/user_item_matrix copy.txt"

# Define k
k = 3  # Number of highest values to select
noItem = "7"


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

# Convert 'key' column to integer type
data_B_df["key"] = data_B_df["key"].astype(int)

# Merge data_A_df and data_B_df on 'key'
merged_df = pd.merge(data_A_df, data_B_df, on="key", how="inner")

# Sort merged DataFrame based on 'value_x' and select the top k elements
sorted_merged_df = merged_df.sort_values(by="value_x", ascending=False).head(k)

# Retrieve corresponding values from sorted_merged_df
corresponding_values = sorted_merged_df[["key", "value_y"]].values.tolist()

# Write new centroids to file
with open("./clustering/proposal_2_chi2/output/centroids.txt", "w") as file:
    for i in corresponding_values:
        file.writelines(f"{i[0]}\t{i[1]}\n")
        i[1] = [float(coor.strip().split(";")[1]) for coor in (i[1].strip().split("|"))]

print(corresponding_values)
