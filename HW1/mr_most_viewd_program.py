
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

PROGRAM_RE = re.compile(r'[a-zA-Z0-9]+$')

class MRMostViewdProgram(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_split)
        ]

    def mapper_split(self, _, line):
        row = re.split(r',(?=(?:(?:[^"]*"){2})*[^"]*$)', line.strip())
        # Check if the row contains a quoted string
        for i in range(len(row)):
            if re.match(r'^".*"$', row[i]):
                # Remove the quotes from the quoted string and split it by commas
                row[i] = [re.sub(r'^"|"$', '', row[i])]
                row[i] = re.split(r',', row[i][0])

        yield None, row

    def mapper_get_prog(self, _,line):
        cols = line.strip().split(',')
        title,genre,air_time = cols[1], cols[2:-3], cols[-2]
        yield (title,genre,air_time), 1
        # try:
        #     time_hours = int(event_time[:2])
        # except ValueError:
        #     # skip lines with invalid time strings
        #     return
        # if 20 <= time_hours < 23:
        #     if int(station_num) % 2 == 0:
        #         yield prog_code, 1


    def combiner_count_prog(self, prog, counts):
        # sum the program code we've seen so far
        yield (prog, sum(counts))

    def reducer_count_prog(selfself, prog, counts):
        yield None, (sum(counts), prog)

    def reduce_find_max_prog(self, _, prog_count_pairs):
        yield max(prog_count_pairs)

if __name__ == '__main__':
    MRMostViewdProgram.run()
