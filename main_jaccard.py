import time
from Jaccard_hadoop.main_hadoop import run_jaccard
from custom_util import (
    env_dict,
    write_data_to_file,
)

start = time.time()

HADOOP_PATH = env_dict["hadoop_path"]

input_path = f"{HADOOP_PATH}/input/input_file.txt"

result = []
result_data = run_jaccard(input_path)
result.append(result_data)
result = [line for row in result for line in row]

output_path = f"./hadoop_output/jaccard.txt"
write_data_to_file(output_path, result)

end = time.time()

print("Hadoop Jaccard run time: " + str(end - start))


def create_combinations(arr1, arr2):
    df1 = pd.DataFrame(np.vstack(arr1), columns=["item", "rating", "time"], dtype="int")
    df2 = pd.DataFrame(np.vstack(arr2), columns=["item", "rating", "time"], dtype="int")
