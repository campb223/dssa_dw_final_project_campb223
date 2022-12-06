import cloudpickle as cpickle
from dexxy.common.logger import LoggingStuff
from dexxy.common.graphs import DAG
from dexxy.common.queues import QueueWarehouse
from dexxy.common.tasks import Task, createTask
from dexxy.common.executors import Worker
from dexxy.common.exceptions import DependencyError, NotFoundError
from typing import Any, List, Literal, Tuple

class Pipeline(DAG, LoggingStuff):
    """A Directed Acyclic MultiGraph based Pipeline for Data Processing. """

    pipeline_id = 0

    def __init__(self, steps: List[Task] = [], type: Literal['default'] = 'default'):

        Pipeline.pipeline_id += 1
        super().__init__()
        self.pid = Pipeline.pipeline_id
        self.steps = [step if isinstance(step, Pipeline) else createTask(step) for step in steps]
        self.type = type
        self._log = self.logger
        self.queue = QueueWarehouse.warehouse(type=type)
        self._log.info('Initalized Pipeline %s' % self.pid)

    def merge_dags(self, pipeline: "Pipeline") -> None:
        """
        Allow a Pipeline object to receive another Pipeline object by merging two Graphs together and preserving attributes.

        Args:
            pipeline (Pipeline): A Pipeline object that contains Task(s). 
        """
        
        pipeline.compose(self)
        G = pipeline.dag
        self.dag = self.merge(G, self.dag)
        self.repair_attributes(G, self.dag, 'tasks')

    def proc_pipeline_dep(self, idx, task, dep):
        """
        Process Dependencies that contain another Pipeline

        Args:
            idx (_type_): _description_
            task (_type_): _description_
            dep (_type_): _description_

        Raises:
            DependencyError: _description_

        Returns:
            _type_: _description_
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
        """Process Dependencies that contain a reference to another task"""
        # this is very ugly and needs to be refactored
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

    def proc_task_dep(self, task, dep, input_pipe):
        """Processes Dependencies that contain a subclass of a Task."""

        # Lookup dependent task from the current pipeline
        pipe = self
        if dep.tid not in self.dag:
            pipe = input_pipe
        if pipe.dag.nodes[dep.tid].get('tasks', None) is not None:
            for k in pipe.dag.nodes[dep.tid]['tasks'].keys():
                for tsk in pipe.steps:
                    if k == tsk.tid:
                        task.related.append(k)
        else:
            raise DependencyError(f'{dep} was not found in {self.__name__}, check pipeline steps.')

        return (task, dep)

    def process_dep(self, idx: int, task: Task, dep: Any, input_pipe: "Pipeline") -> Tuple[Task, Task]:
        """Basic Factory function for processing dependencies.
        """
        if isinstance(dep, Pipeline):
            return self.proc_pipeline_dep(idx, task, dep)
        elif isinstance(dep, str):
            return self.proc_named_dep(idx, task, dep, input_pipe)
        elif issubclass(type(dep), Task):
            return self.proc_task_dep(task, dep, input_pipe)
        else:
            raise TypeError("Invalid Dependencies found in {self.__name__}: Task {task.__name__} ")

    def get_task_by_name(self, name: str) -> Task:
        """Retrieves an Task from the DAG using its name
        Args:
            name (str): The name of the Task
        Raises:
            NotFoundError: Complains if the task could not be found
        Returns:
            Task: The task that matches the name parameter.
        """
        for tsk_attrs in self.get_all_attributes(name='tasks'):
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
        """Enqueues all Tasks from the constructed DAG in topological sort order
        """
        # Compile steps into the DAG if not already compiled
        if self.is_empty():
            self.compose()

        self.queue = QueueWarehouse.warehouse(self.type)
        # Begin Enqueuing all Tasks in the DAG
        nodes = self.get_all_nodes()
        # Get Topological sort of Task Nodes by Id
        for task_node_id in self.topological_sort():
            # Lookup each task in a node
            n_attrs = nodes[task_node_id]
            # Enqueue Tasks & update status
            for v in n_attrs['tasks'].values():
                self.queue.put(v)
                v.updateStatus('Queued')

    def run(self) -> Any:
        """Allows for Local Execution of a Pipeline Instance. Good for Debugging
        for advanced features and concurrency support use submit"""
        self.result_queue = QueueWarehouse.warehouse(self.type)
        # If Queue is empty, populate it
        if self.queue.empty():
            self.collect()

        # Setup Default Worker
        worker = Worker(taskQueue=self.queue, resultQueue=self.result_queue)

        # Start execution of Tasks
        self._log.info('Starting Execution')
        worker.start()
        worker.end()