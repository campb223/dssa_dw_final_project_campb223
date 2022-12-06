from typing import Any, List, Union, TypeVar, Callable, Literal, Tuple
from dexxy.common.logger import LoggingStuff
from dexxy.common.utils import generateUniqueID

Task = TypeVar('Task')
Pipeline = TypeVar('Pipeline')

class Task(LoggingStuff):
    
    def __init__(self, func: Callable, kwargs: dict = {}, dependsOn: List = None, name: str = None) -> None:
        """
        Initalization of the class Task. To inilizatize it will look like:
            Task(createCursor,
                kwargs={'path': databaseConfig, 'section': section},
                dependsOn=None,
                name='createCursor')
                
        It accepts input variables to know how to call other functions, which varibles to pass, and what other Tasks it depends on to execute. 
        By default the status is "Not Started", a logger is generated, and nothing is related to this. 

        Args:
            func (Callable): The function to call when operating on this Task. 
            kwargs (dict, optional): This is the input paramters to the specified function. Defaults to {}.
            dependsOn (List, optional): List of other Tasks this is dependent on to execute. Defaults to None.
            name (str, optional): Name of the Task. Defaults to None.
        """
        
        self.func = func
        self.kwargs = kwargs
        self.dependsOn = dependsOn
        self.name = name
        self.status = "Not Started"
        self.related = []
        self.result = None
        self.tid = generateUniqueID()
        self._log = self.logger
        self._log.info('Initalized Task %s' % self.name)
    
    def updateStatus(self, status: Literal['Not Started', 'Queued', 'Running', 'Completed', 'Failed'] = 'Not Started') -> None:
        """
        A function to allow updaing the status of a Task.status. It allows us to track the progress of a Task through execution. 

        Args:
            status (Literal[Not Started, Queued, Running, Completed, Failed], optional): Allows you to pass one of these options when updating the status. Defaults to 'Not Started'.
        """
        self.status = status
        
    def run(self, inputs:tuple):
        """
        A function that runs the func with kwargs specificed in the Task

        Returns:
            Any: If there is a df, list, etc. to return by the specific function, it will return this. 
        """
        
        try:
            self.result = self.func(*inputs, **self.kwargs)
        except Exception as error:
            self._log.exception(error, exc_info=True, stack_info=True)
            
def getTaskResult(task) -> Tuple[Any]:
    """
    Takes in a node in the Task with a list of UUIDs to lookup. Then looks up the data required to run a task using the provided list of UUIDs. 

    Returns:
        Tuple[Any]: Returns the outputs from the Tasks func call. Could be a cursor, df, etc. 
    """
    
    inputs = []
    
    # If there's a task
    if task is not None:
        data = task.result
        if data is not None:
            inputs.append(data)
    return tuple(inputs)

def createTask(inputs: Union[Task, tuple]):
    """
    A function to create a new Task. Checks what type of instance is passed in. 
        If the instance is a Task -- returns inputs. 
        If the instance is a Tuple -- returns Task.
        Otherwise throws an error. 

    Raises:
        TypeError: If the instance type is not a Task or Tuple -- raise TypeError. 

    Returns:
        Task: An object of Task class. 
    """
    
    if isinstance(inputs, Task):
        return inputs
    elif isinstance(inputs, Tuple):
        return Task(*inputs)
    else:
        raise TypeError('Step must be a Task, Pipeline, or Tuple.')
