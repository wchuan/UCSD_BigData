import re,pickle,base64,zlib
from mrjob.job import MRJob
import pandas as pd
import numpy as np
import sklearn as sk
import random

Djoined = pd.DataFrame.from_csv('labeled_stations.csv')

class PCA_weather(MRJob):

    def mapper(self, _, line):
        elements = line.split(',')
        find_ind = Djoined[Djoined['station'] == elements[0]]
        #if len(find_ind) != 0 and (elements[1] == 'TMAX' or elements[1] == 'TMIN'):
        if len(find_ind) != 0 and elements[1] == 'TMAX':
            ind = find_ind.index[0]
            region = Djoined.loc[ind]['label'] 
            yield region, elements[3:]
    
    def reducer(self, region, matrixs):
        M = pd.DataFrame(matrixs)
        M[M == ''] = float('NaN')
        M = M.astype(float)
        M = M.transpose()
        (columns,rows)= np.shape(M)
        Mean = np.mean(M, axis=1).values
        C=np.zeros([columns,columns])  
        N=np.zeros([columns,columns])

        for i in range(rows):
            row = M.iloc[:,i] - Mean
            outer = np.outer(row,row)
            valid = np.isnan(outer) == False
            C[valid] = C[valid]+ outer[valid]
            N[valid] = N[valid]+ 1
        valid_outer = np.multiply(1 - np.isnan(N),N>0)
        cov = np.divide(C,N)
        cov = np.multiply(cov, valid_outer)
        U, D, V = np.linalg.svd(cov)
        cum_sum = np.cumsum(D[:])/sum(D)
        for i in range(len(cum_sum)):
            if cum_sum[i] >= 0.95:
                ind = i 
                break
        yield region, ind
        #yield region, [ind, U.tolist(), C.tolist(), N.tolist()]

if __name__ == '__main__':
    PCA_weather.run()