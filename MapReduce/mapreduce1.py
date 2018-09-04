# mrjob is used to run python scripts
from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesBasedOnRating(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')# data set is delimited by tabs
        yield rating, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MoviesBasedOnRating.run()