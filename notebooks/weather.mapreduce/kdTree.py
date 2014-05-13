from collections import namedtuple
from operator import itemgetter
from pprint import pformat

class Node(namedtuple('Node', \
            'location left_child right_child')):
    def __repr__(self):
        return pformat(tuple(self))

def kdtree(point_list, depth=0):
    if len(point_list) == 0:
        return None
    # k is the dimension of data
    k = len(point_list[0])
    # Select axis based on depth so that axis cycles through all valid values
    axis = depth % k
    point_list.sort(key=itemgetter(axis))
    median = len(point_list) // 2

    return Node(point_list[median], \
                kdtree(point_list[:median], depth+1), \
                kdtree(point_list[median+1:], depth+1) )

def main():
    point_list = [(2,3), (5,4), (9,6), (4,7), (8,1), (7,2)]
    tree = kdtree(point_list)
    print(tree)

if __name__ == '__main__':
    main() 



