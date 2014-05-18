from mrjob.job import MRJob
from mrjob.protocol import *
import numpy as np

class PCA(MRJob):

    def mapper(self, _, line):
        X_raw = line.split(',')
        X = np.array(X_raw[3:])
        X = X - np.mean(X)
        yield , np.outer(X)
    
    def reducer(self, station, cov):
        




if __name__ == '__main__':
    PCA.run()
