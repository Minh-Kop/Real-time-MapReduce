from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class CreateUserPairs(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(CreateUserPairs, self).configure_args()
        self.add_file_arg("--users-path", help="Path to the users file")

    def create_user_list(self, filename):
        user_ids = []
        with open(filename, "r") as file:
            for line in file:
                user_id = line.strip().split("\t")[0]
                user_ids.append(int(user_id))
        return user_ids

    def mapper_init(self):
        users_path = self.options.users_path
        self.users = self.create_user_list(users_path)

    def mapper(self, _, line):
        current_user = (line.strip().split("\t"))[0]
        current_user = int(current_user)

        for user in self.users:
            if user > current_user:
                yield f"{current_user};{user}", "0"


if __name__ == "__main__":
    #     import sys
    #
    #     sys.argv[1:] = [
    #         "input/avg_ratings_2.txt",
    #         "--users-path",
    #         "input/avg_ratings_2.txt",
    #     ]
    CreateUserPairs().run()
