import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import os


class rating_commodity(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(rating_commodity, self).configure_args()
        self.add_file_arg('--users-path', help='Path to the users file')

    def create_user_list(self, filename):
        user_ids = []
        with open(filename, 'r') as file:
            for line in file:
                user_id = line.strip()  # Remove leading/trailing whitespaces and newlines
                user_ids.append(int(user_id))
        return user_ids

    def rating_commodity_mapper_init(self):
        # users_path = os.path.join(os.path.dirname(__file__), 'users.txt')
        users_path = self.options.users_path
        self.users = self.create_user_list(users_path)

    def rating_commodity_mapper(self, _, line):
        currentUser, item = (line.rstrip().split('\t'))[0].strip().split(';')
        currentUser = int(currentUser)

        for user in self.users:
            if user < currentUser:
                yield f'{user};{currentUser}', item
            elif user > currentUser:
                yield f"{currentUser};{user}", item

    def rating_commodity_reducer(self, key, value):
        user1, user2 = key.strip().split(';')
        value = list(value)
        uniqueValues = set(value)

        yield f'{user1};{user2}', f'{len(value) - len(uniqueValues)};rc'

    def steps(self):
        return [
            MRStep(mapper_init=self.rating_commodity_mapper_init,
                   mapper=self.rating_commodity_mapper, reducer=self.rating_commodity_reducer)
        ]


if __name__ == '__main__':
    output_path = os.path.join(os.getcwd(), 'output1.txt')
    # sys.argv.extend(['input_file.txt', '>', f'file://{output_path}'])
    sys.argv[1:] = [
        '--users-path', '../users.txt',  # Đường dẫn đến tệp users.txt
        '../input_file.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    rating_commodity().run()
