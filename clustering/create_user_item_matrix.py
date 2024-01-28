import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class UserItemMatrix(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def create_user_item_matrix_mapper(self, _, line):
        key, value = line.strip().split('\t')
        user, item = key.strip().split(';')
        rating = value.strip().split(';')[0]

        yield user, f"{item};{rating}"

    def configure_args(self):
        super(UserItemMatrix, self).configure_args()
        self.add_file_arg('--items-path', help='Path to the items file')
        self.add_file_arg('--avg-ratings-path',
                          help='Path to the avg ratings file')

    def create_item_list(self, filename):
        items = []
        with open(filename, 'r') as file:
            for line in file:
                # item = line.strip(' \t\n')
                item = line.strip()  # Remove leading/trailing whitespaces and newlines
                items.append(float(item))
        return items

    def create_avg_ratings(self, filename):
        avg_ratings = {}
        with open(filename, 'r') as file:
            for line in file:
                user, avg_rating = line.strip().split('\t')
                avg_rating = float(avg_rating)
                avg_ratings[user] = avg_rating
        return avg_ratings

    def create_user_item_matrix_reducer_init(self):
        items_path = self.options.items_path
        avg_ratings_path = self.options.avg_ratings_path
        self.items = self.create_item_list(items_path)
        self.avg_ratings = self.create_avg_ratings(avg_ratings_path)

    def create_user_item_matrix_reducer(self, user, values):
        values = [value.strip().split(';') for value in values]
        avg_rating = self.avg_ratings[user]
        result = ''

        for item in self.items:
            found = False
            for value in values:
                user_item, rating = value
                user_item = float(user_item)
                if user_item == item:
                    result += f"{item};{rating}"
                    found = True
                    break
            if not found:
                result += f"{item};{avg_rating}"
            result += '|'
        result = result[:-1]
        yield user, result

    def steps(self):
        return [
            MRStep(mapper=self.create_user_item_matrix_mapper,
                   reducer_init=self.create_user_item_matrix_reducer_init, reducer=self.create_user_item_matrix_reducer)
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '../input_file.txt',
        '--items-path', 'items.txt',
        '--avg-ratings-path', 'calculate_avg_rating.txt',
    ]
    UserItemMatrix().run()
