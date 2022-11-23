# Basically just use what he has -- he said don't need to do anything else really.
from networkx import MultiDiGraph

# *********
# after topological sort you'll have sorted task of nodes. First task you pop off stack, is the first task you put in the queue

class DAG:
    
    def __init__(self, **attrs) -> None:
        self.dag = MultiDiGraph()
        self.attrs = attrs
        
        
        
    # if you try to combine two graphs together and have same index, 
    # Networkx will overwrite one of them 