from dataclasses import dataclass
from collections import deque

@dataclass
class DAG:
    graph: dict

    def __init__(self):
        self.graph = {}

    def in_degrees(self):
        in_degrees = {node: 0 for node in self.graph}
        for node in self.graph:
            for pointed in self.graph[node]:
                in_degrees[pointed] += 1
        return in_degrees

    def sort(self):
        in_degrees = self.in_degrees()
        to_visit = deque([node for node in self.graph if in_degrees[node] == 0])
        searched = []
        while to_visit:
            node = to_visit.popleft()
            searched.append(node)
            for pointer in self.graph[node]:
                in_degrees[pointer] -= 1
                if in_degrees[pointer] == 0:
                    to_visit.append(pointer)
        if len(searched) != len(self.graph):
            raise Exception("Cycle detected")
        return searched

    def add(self, node, to=None):
        if node not in self.graph:
            self.graph[node] = []
        if to:
            if to not in self.graph:
                self.graph[to] = []
            self.graph[node].append(to)
        if len(self.sort()) != len(self.graph):
            self.graph[node].remove(to)
            if not self.graph[node]:
                del self.graph[node]
            if not self.graph[to]:
                del self.graph[to]
            raise Exception("Cycle detected")

@dataclass
class Pipeline:
    tasks: DAG

    def __init__(self):
        self.tasks = DAG()

    def task(self, depends_on=None):
      def inner(f):
        self.tasks.add(f)
        if depends_on:
          self.tasks.add(depends_on, f)
        return f
      return inner

    def run(self, param=None):
      order = self.tasks.sort()
      outputs = {}    
      for n, task in enumerate(order):
        for k,v in self.tasks.graph.items():
          if task in v:
            outputs[task] = task(outputs[k])        
        if task not in outputs:
          if n == 0 and param:
            outputs[task] = task(param)
          else:
            outputs[task] = task()               
      return outputs

