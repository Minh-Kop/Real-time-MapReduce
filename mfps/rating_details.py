import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class rating_details(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(rating_details, self).configure_args()
        self.add_file_arg('--combinations-path',
                          help='Path to combinations file')

    def create_combinations_list(self, filename):
        combinations = []
        with open(filename, 'r') as file:
            for line in file:
                combination = line.strip()  # Remove leading/trailing whitespaces and newlines
                combinations.append(combination)
        return combinations

    def rating_details_mapper_init(self):
        combinations_path = self.options.combinations_path
        self.combinations = self.create_combinations_list(combinations_path)

    def rating_details_mapper(self, _, line):
        users, value = line.strip().split('\t')
        user_1, user_2 = users.strip().split(';')
        avg_rating = value.strip().split(';')[1]

        for combination in self.combinations:
            c_users, c_value = combination.strip().split('\t')
            c_user_1, c_user_2 = c_users.strip().split(';')
            ratings = c_value.strip().split('|')[0]

            if ((user_1 == c_user_1 and user_2 == c_user_2) or (user_1 == c_user_2 and user_2 == c_user_1)):
                yield users, f'{avg_rating};{ratings}'

    def rating_details_reducer(self, users, values):
        values = list(values)
        values = [value.strip().split(';') for value in values]

        count = 0
        liking_threshold = 3.5

        for line in values:
            liking_threshold, user1_rating, user2_rating = line
            liking_threshold = float(liking_threshold)
            user1_rating = float(user1_rating)
            user2_rating = float(user2_rating)
            if (user1_rating > liking_threshold and user2_rating > liking_threshold):
                count += 1
            elif (user1_rating < liking_threshold and user2_rating < liking_threshold):
                count += 1

        yield users, f'{count};rd'

    def steps(self):
        return [
            MRStep(mapper_init=self.rating_details_mapper_init,
                   mapper=self.rating_details_mapper,
                   reducer=self.rating_details_reducer)
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        # Đường dẫn đến tệp create_combinations.txt
        '--combinations-path', './create_combinations.txt',
        './rating_usefulness.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    rating_details().run()
