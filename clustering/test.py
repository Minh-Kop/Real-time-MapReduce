import numpy as np
from importance import Importance
from most_importance import MostImportance

if __name__ == '__main__':
    mr_job = Importance(args=['../input_file.txt'])
    with mr_job.make_runner() as runner:
        runner.run()
        fw = open('test.txt', 'w')
        for key, value in mr_job.parse_output(runner.cat_output()):
            fw.writelines(f'{key}\t{value}')
