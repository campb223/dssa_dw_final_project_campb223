import pickle
from dexxy.common.logger import LoggingStuff
from dexxy.common.queues import QueueWarehouse
from dexxy.common.tasks import Task, createTask
from dexxy.common.workers import Worker
from dexxy.common.scheduler import Scheduler
from dexxy.common.exceptions import DependencyError, NotFoundError, CircularDependencyError, MissingDependencyError
from typing import Any, List, Literal, Tuple, Dict, Type
from uuid import uuid4
from networkx import MultiDiGraph, compose, is_directed_acyclic_graph, is_weakly_connected, topological_sort


class Pipeline(LoggingStuff):
    """A Directed Acyclic MultiGraph based Pipeline for Data Processing. """

    pipeline_id = 0

    def __init__(self, steps: List[Task] = [], type: Literal['default'] = 'default'):

        Pipeline.pipeline_id += 1
        self.pid = Pipeline.pipeline_id
        self.dag = MultiDiGraph()
        self.scheduler = Scheduler()
        self.steps = [step if isinstance(step, Pipeline) else createTask(step) for step in steps]
        self.type = type
        self._log = self.logger
        self.queue = QueueWarehouse.warehouse(type=type)
        self._log.info('Initalized Pipeline %s' % self.pid)

    def validate_dag(self) -> None:
        """
        Validates Pipeline is constructed properly. Essentially checks to see if it is a DAG and is NOT weakly connected. 
        
        Raises:
            CircularDependencyError: Error if DAG contains cycles
            MissingDependencyError: Error raised if DAG contains disconnected nodes
        """

        # Validate DAG does not have cycles
        if not is_directed_acyclic_graph(self.dag):
            raise CircularDependencyError("DAG Contains Cycles.")

        # Validate DAG does not have weakly connected nodes
        if not is_weakly_connected(self.dag):
            raise MissingDependencyError("DAG Contains Weakly Connected Nodes")
        
    def merge_dags(self, pipeline: "Pipeline") -> None:
        """
        Allow a Pipeline object to receive another Pipeline object by merging two Graphs together and preserving attributes.
        
        Returns a new graph of self composed with G. Composition is the simple union of the node sets and edge sets. The node sets of G and self.dag do not need to be disjoint. \
            Note: Edges in G that have the same edge key as edges in H will be overwritten with edges from self.dag.
            
        Args:
            self (MultiDiGraph): First MultiDiGraph Instance
            pipeline (Pipeline): A Pipeline object that contains Task(s). 
        """
        
        pipeline.compose(self)
        G = pipeline.dag
        self.dag = compose(G, self.dag)
        self.repair_attributes(G, self.dag, 'tasks')

    def proc_pipeline_dep(self, idx, task, dep):
        """
        Process Dependencies that contain another Pipeline

        Args:
            idx (int): The iteration index we're currently processing on. 
            task (Task): The Task to process dependencies of. 
            dep (Any): The dependncy to evaluate

        Raises:
            DependencyError: Thrown if the dependency was not found in the Pipeline. 

        Returns:
            (task, dep_task): The Task and dependent Task
        """
        # gets the last step from the pipeline dependency
        dep_task = dep.steps[-1]

        # Update the task with uuids of related runs
        if dep.dag.nodes[dep_task.tid].get('tasks', None) is not None:
            for k in dep.dag.nodes[dep_task.tid]['tasks'].keys():
                task.related.append(k)
        else:
            raise DependencyError(f'{dep} was not found in {self.__name__}, check pipeline steps.')

        # Replace the Pipeline References with Task Reference
        task.dependsOn[idx] = dep_task

        return (task, dep_task)

    def proc_named_dep(self, idx: int, task: Task, dep: str, input_pipe: "Pipeline"):
        """
        Process Dependencies that contain a reference to another task

        Args:
            idx (int): The iteration index we're currently processing on. 
            task (Task): The Task to process dependencies of. 
            dep (str): The dependncy to evaluate
            input_pipe (Pipeline): A Pipeline which could a previous pipeline we'll need to pull dependencies from. 

        Raises:
            DependencyError: Thrown if the dependency was not found in the Pipeline. 

        Returns:
            (task, dep_task): The Task and dependent Task
        """
        
        try:
            dep_task = self.get_task_by_name(name=dep)
            dag = self.dag
        except BaseException:
            dep_task = input_pipe.get_task_by_name(name=dep)
            dag = input_pipe.dag

        task.dependsOn[idx] = dep_task

        # Lookup dependent task from the current pipeline or the called pipeline
        if dag.nodes[dep_task.tid].get('tasks', None) is not None:
            for k in dag.nodes[dep_task.tid]['tasks'].keys():
                task.related.append(k)
        else:
            raise DependencyError(f'{dep_task} was not found in {self.__name__}, check pipeline steps.')

        return (task, dep_task)

    def process_dep(self, idx: int, task: Task, dep: Any, input_pipe: "Pipeline") -> Tuple[Task, Task]:
        """
        Basic Factory function for processing dependencies.

        Args:
            idx (int): The iteration index we're currently processing on. 
            task (Task): The Task to process dependencies of. 
            dep (Any): The dependncy to evaluate
            input_pipe (Pipeline): A Pipeline which could a previous pipeline we'll need to pull dependencies from.

        Raises:
            TypeError: Raised if a Type we haven't accounted for is passed in. 

        Returns:
            Tuple[Task, Task]: The Task and dependent Task
        """
        
        if isinstance(dep, Pipeline):
            return self.proc_pipeline_dep(idx, task, dep)
        elif isinstance(dep, str):
            return self.proc_named_dep(idx, task, dep, input_pipe)
        else:
            raise TypeError("Invalid Dependencies found in {self.__name__}: Task {task.__name__} ")

    def get_task_by_name(self, name: str) -> Task:
        """
        Retrieves a Task from the DAG using its name
        
        Args:
            name (str): The name of the Task
        Raises:
            NotFoundError: Complains if the task could not be found
        Returns:
            Task: The task that matches the name parameter.
        """
        for tsk_attrs in dict(self.dag.nodes(data='tasks', default=None)).values():
            if tsk_attrs is not None:
                for tsk in tsk_attrs.values():
                    if tsk.name == name:
                        return tsk

        raise NotFoundError(f"{name} was not found in the DAG")

    def compose(self, input_pipe: "Pipeline" = None) -> None:
        """
        Compose the DAG from steps provided to the pipeline
        """
        # For each task found in steps
        for task in self.steps:
            # Process the task with a special call if it is a Pipeline Instance
            if isinstance(task, Pipeline):
                self.merge_dags(task)
                continue

            # Process the dependencies
            if task.dependsOn is not None:
                for idx, dep_task in enumerate(task.dependsOn):
                    # Process the dependency
                    task, dep_task = self.process_dep(idx, task, dep_task, input_pipe)

                    # Add edge to DAG using task id as an edge key
                    self.add_edge_to_dag(self.pid, dep_task.tid, task.tid, task.tid)

            # Add Task to node with related keys
            task.related = list(dict.fromkeys(task.related).keys())
            self.add_node_to_dag(task)

        # Validates DAG was constructed properly
        self.validate_dag()

    def collect(self) -> None:
        """
        Enqueues all Tasks from the constructed DAG in topological sort order
        """

        # Defining a Queue. 
        self.queue = QueueWarehouse.warehouse(self.type)
        
        # Begin Enqueuing all Tasks in the DAG
        nodes = dict(self.dag.nodes(data=True))
        
        # Get Topological sort of Task Nodes by Id
        for task_node_id in list(topological_sort(G=self.dag)):
            # Lookup each task in a node
            n_attrs = nodes[task_node_id]
            # Enqueue Tasks & update status
            for v in n_attrs['tasks'].values():
                self.queue.put(v)
                v.updateStatus('Queued')

    def run(self) -> Any:
        """
        Allows for Local Execution of a Pipeline Instance. When called, a queue is generated, the Worker is set up, log shows beginning execution, and the worker is started. 
        Once completed, the worker is ended (by deleting the result queue)
        """
        
        self.result_queue = QueueWarehouse.warehouse(self.type)

        # Setup Default Worker
        worker = Worker(taskQueue=self.queue, resultQueue=self.result_queue)

        # Start execution of Tasks
        self._log.info('Starting Execution')
        worker.start()
        
        # Ends execution of Tasks
        worker.end()
        
    def add_node_to_dag(self, task: Type[Task] = None, properties: Dict = None) -> None:
        """
        Adds a new Node to the DAG with attributes
        
        Args:
            task (Type[Task], optional): Task Instance. Defaults to None.
            properties (Dict, optional): User Properties. Defaults to None.
        """
        # if the node already exists
        if task.tid in list(self.dag.nodes):
            existing = dict(self.dag.nodes(data='tasks')).get(task.tid, None)
            if existing is not None:
                updates = existing.update({task.tid: task})
                return
            else:
                updates = {task.tid: task}
        else:
            updates = {task.tid: task}
        # Add a new node to the DAG
        self.dag.add_nodes_from([(task.tid, {"id": task.tid, "tasks": updates, "properties": properties})])

    def add_edge_to_dag(self, pid: int, tid_from: int, tid_to: int, activity_id: uuid4) -> None:
        """
        Adds an edge between two nodes to the DAG
        
        Args:
            tid_from (int): The dependency Task unique ID "tid"
            tid_to (int): The Task unique ID "tid"
            activity_id (uuid): Task Id used to define the edge
        """
        # Add the edge to the DAG
        self.dag.add_edges_from([(tid_from, tid_to, activity_id, {"pid": pid, "tid_from": tid_from, "tid_to": tid_to,})])

    def repair_attributes(self, G: MultiDiGraph, H: MultiDiGraph, attr: str) -> None:
        """
        Preserved node attributes that may be overwritten when using merge. Note: this method only works if the attribute being preserved is a dictionary
        
        Args:
            G (MultiDiGraph): left graph
            H (MultiDiGraph): right graph
            attr (str): name of attribute
        """
        
        # Looping through each node in G
        for node in G.nodes():
            # If the node is also in H
            if node in H:
                # If this nodes has an attribute(s)
                if self.dag.nodes[node].get(attr, None) is not None:
                    # Save those attributes
                    attr_G = G.nodes[node].get(attr)
                    if attr_G is not None:
                        attr_H = H.nodes[node].get(attr)
                        # If the attributes are = then we're good. Continue to next iteration. 
                        if attr_G == attr_H:
                            continue
                        # Otherwise, repair the attirbutes on this node. 
                        else:
                            attr_G.update(attr_H)
                            self.dag.nodes[node][attr] = attr_G
                            
    def saveDAG(self, filename):
        """
        In order to save a DAG and execute later, we take in the pipeline, seralize the DAG with pickle, then save the dag to a file/path specified in filename. 

        Args:
            filename (str): The path and filename to save the DAG as. 
                Example filename='dags/dvd_rental' --> This would save the DAG in a folder called 'dags' as 'dvd_rental'

        Returns:
            self: returns itself back to where it was called. Allows for additional execution on this Pipeline. 
        """
        
        pipeline_bytes = pickle.dumps(self.dag)
        
        with open(filename, 'wb') as f:
            f.write(pipeline_bytes)
            
        return self
    
    def openDAG(self, filename):
        """
        Reads in a DAG from a filename provided. This is necessary to begin processing the DAG by the scheduler. 

        Args:
            filename (str): The path and filename to load the DAG froom. 
                Example filename='dags/dvd_rental' --> This would load the DAG from a folder called 'dags' with the files actual name = 'dvd_rental'

        Returns:
            self: returns itself back to where it was called. Allows for additional execution on this Pipeline. 
        """
        with open(filename, 'rb') as f:
            pipline_bytes = f.read()
            
        self.dag = pickle.loads(pipline_bytes)
        return self