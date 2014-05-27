from __future__ import division
from mrjob.job import MRJob

class ValidStation(MRJob):
    def mapper1(self, _, line):
        elements = line.split(',')
        if elements[1] == 'TMIN' or elements[1] == 'TMAX':
            yield (elements[0], elements[2]), elements[3:]

    def reducer1(self, pair, vectors):
        vectors = list(vectors)
        if len(vectors) < 2:
            yield pair[0], 0
        else:
            day_count = 0 
            for i in range(len(vectors[0])):
                if vectors[0][i] != '' and vectors[1][i] != '': 
                    day_count += 1
            if day_count / len(vectors[0]) < 0.5:
                yield pair[0], 0
            else:
                yield pair[0], 1
    
    def reducer2(self, station, is_valid):
        sum_valid = sum(is_valid)
        if sum_valid != 0:
            yield station, sum_valid
    
    def steps(self):
        return [self.mr(mapper=self.mapper1, reducer=self.reducer1),
                self.mr(reducer=self.reducer2)]

if __name__ == '__main__':
    ValidStation.run()