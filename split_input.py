from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class SplitInput(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(SplitInput, self).configure_args()
        self.add_passthru_arg('--cid', type=str, default=1)

    def split_input_mapper(self, _, line):
        variables = line.strip().split('&')
        key, value = variables[0].strip().split('\t')

        if (len(variables) == 1):
            user, item = key.strip().split(';')

            yield f'{user}', f'{item};{value}'
        else:
            yield f'{key}', f'{variables[1]}'

    def split_input_reducer(self, key, values):
        values = list(values)

        centroid = self.options.cid
        if centroid in values:
            for i in values:
                if i != centroid:
                    item, rating, time = i.strip().split(';')
                    yield f'{key};{item}', f'{rating};{time}'

    def steps(self):
        return [
            MRStep(mapper=self.split_input_mapper,
                   reducer=self.split_input_reducer),
        ]
