from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class RatingCommodity(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(RatingCommodity, self).configure_args()
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
        current_user, item = (line.strip().split("\t"))[0].strip().split(";")
        current_user = int(current_user)

        for user in self.users:
            if user < current_user:
                yield f"{user};{current_user}", item
            elif user > current_user:
                yield f"{current_user};{user}", item

    def reducer(self, users, items):
        user1, user2 = users.strip().split(";")
        items = list(items)
        unique_items = set(items)

        yield f"{user1};{user2}", f"{len(items) - len(unique_items)};rc"


if __name__ == "__main__":
    RatingCommodity().run()
