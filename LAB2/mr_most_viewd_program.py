
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

PROGRAM_RE = re.compile(r'[a-zA-Z0-9]+$')

class MRMostViewdProgram(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_prog,
                   combiner=self.combiner_count_prog,
                   reducer=self.reducer_count_prog),
            MRStep(reducer=self.reduce_find_max_prog)
        ]
    def mapper_get_prog(self, _,line):
        cols = line.strip().split(',')
        event_time, station_num, prog_code = cols[3:6]
        try:
            time_hours = int(event_time[:2])
        except ValueError:
            # skip lines with invalid time strings
            return
        if 20 <= time_hours < 23:
            if int(station_num) % 2 == 0:
                yield prog_code, 1


    def combiner_count_prog(self, prog, counts):
        # sum the program code we've seen so far
        yield (prog, sum(counts))

    def reducer_count_prog(selfself, prog, counts):
        yield None, (sum(counts), prog)

    def reduce_find_max_prog(self, _, prog_count_pairs):
        yield max(prog_count_pairs)

if __name__ == '__main__':
    MRMostViewdProgram.run()
