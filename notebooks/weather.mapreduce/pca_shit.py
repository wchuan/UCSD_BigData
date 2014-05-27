#!/usr/bin/python
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
import re,pickle,base64,zlib
from mrjob.job import MRJob
from sys import stderr

import pandas as pd
import numpy as np
import sklearn as sk
import random
import gzip
import pickle

class PCA_weather(MRJob): 
    
    def configure_options(self):
        super(PCA_weather,self).configure_options()
        self.add_file_option('--stations')
    
    def mapper_init(self):
        f = gzip.open( self.options.stations, "rb" )
        self.station_hash = pickle.load(f)
        f.close()
    
    def mapper(self, _, line):
        try:
            elements = line.split(',')
            #if len(find_ind) != 0 and (elements[1] == 'TMAX' or elements[1] == 'TMIN'):
            if elements[1] == 'TMAX':
                region = int(self.station_hash.get(elements[0]))
                if region != None:
                    yield (region, elements[3:])                                    
        except Exception as e:
            stderr.write('Error: '+ str(e) )
              
    def reducer(self, region, matrixs):
        #M = pd.DataFrame(matrixs)
        #M[M == ''] = float('NaN')
        #M = M.astype(float)
        #M = M.transpose()
        #(columns,rows)= np.shape(M)
        #Mean = np.mean(M, axis=1).values
        #C=np.zeros([columns,columns])  
        #N=np.zeros([columns,columns])

        #for i in range(rows):
        #    row = M.iloc[:,i] - Mean
        #   outer = np.outer(row,row)
        #    valid = np.isnan(outer) == False
        #    C[valid] = C[valid]+ outer[valid]
        #    N[valid] = N[valid]+ 1
            
        #valid_outer = np.multiply(1 - np.isnan(N),N>0)
        #cov = np.divide(C,N)
        #cov = np.multiply(cov, valid_outer)
        #U, D, V = np.linalg.svd(cov)
        #cum_sum = np.cumsum(D[:])/np.sum(D)
        #for i in range(len(cum_sum)):
        #    if cum_sum[i] >= 0.95:
         #       ind = i 
         #       break
        yield region, 1
        #yield region, [ind, U.tolist(), C.tolist(), N.tolist()]

if __name__ == '__main__':
    PCA_weather.run()