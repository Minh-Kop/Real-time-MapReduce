from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class CalculateIntersection(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(CalculateIntersection, self).configure_args()
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
        items = list(items)
        unique_items = set(items)
        number_of_intersected_items = len(items) - len(unique_items)

        yield users, str(number_of_intersected_items)


if __name__ == "__main__":
    CalculateIntersection().run()
