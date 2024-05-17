from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class MFPS(MRJob):
    OUTPUT_PROTOCOL = TextProtocol
    INTERNAL_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        key, value = line.strip().split("\t")
        u1, u2 = key.strip().split(";")

        if value.strip().split(";")[-1] == "rc" or value.strip().split(";")[-1] == "rt":
            yield f"{u2};{u1}", value
        yield key, value

    def reducer(self, key, values):
        values = list(values)
        values = [value.strip().split(";") for value in values]
        mfps = 1

        for line in values:
            sim = float(line[0])
            flag = line[-1]
            if sim == 0:
                if flag == "rc":
                    mfps = 0
                    break
                elif flag == "rd":
                    mfps += 1.1
                continue
            mfps += 1 / sim
        if mfps:
            mfps = 1 / mfps

        yield key, f"{mfps}"


if __name__ == "__main__":
    MFPS().run()
