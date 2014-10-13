from collections import namedtuple



def parse_graph(name='default'):
  graph = {}
  g = open('graph/%s'%name,'r')
  for line in g:
    values = line.split()
    src, dst_list = values[0], values[1:]
    graph[src] = dst_list
  return graph

NodePair = namedtuple("NodePair", ["src", "dst"])

def find_all_paths_for_graph(graph, paths={}):
     for k in graph.keys():
       for v in graph.get(k):
          paths[NodePair(src = k, dst = v)] = find_all_paths_for_nodes(graph, k, v, path=[])
     return paths
       
  
def find_all_paths_for_nodes(graph, start, end, path=[]):
        path = path + [start]
        if start == end:
            return [path]
        if not graph.has_key(start):
            return []
        paths = []
        for node in graph[start]:
            if node not in path:
                newpaths = find_all_paths_for_nodes(graph, node, end, path)
                for newpath in newpaths:
                    paths.append(newpath)
        return paths
      