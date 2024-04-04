import re


def write_data_to_file(filename, data, mode="w"):
    output_file = open(filename, mode)
    for el in data:
        output_file.writelines(el)
    output_file.close()


def get_txt_filename(output_path):
    pattern = "\/([^\/]+)$"
    match = re.search(pattern, output_path)
    if match:
        name = match.group(1)
        return f"{name}.txt"
    return None


def run_mr_job(mr_job_class, input_args, output_path, create_txt_file=False):
    input_args = ["-r", "hadoop"] + input_args
    if output_path:
        input_args = input_args + ["--output-dir", output_path]
    else:
        create_txt_file = False

    mr_job = mr_job_class(args=input_args)
    with mr_job.make_runner() as runner:
        if output_path:
            runner.fs.rm(output_path)
        runner.run()

        data = []
        for key, value in mr_job.parse_output(runner.cat_output()):
            data.append(f"{key}\t{value}")

        if not create_txt_file:
            return data
        filename = get_txt_filename(output_path)
        file_path = f"./input/{filename}"
        write_data_to_file(file_path, data)

        hdfs_path = f"hdfs://localhost:9000/user/mackop/input/{filename}"
        runner.fs.rm(hdfs_path)
        runner.fs.put(file_path, hdfs_path)
