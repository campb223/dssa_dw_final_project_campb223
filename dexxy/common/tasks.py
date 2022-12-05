from typing import List, Union, TypeVar, Any, Callable, Literal, Tuple
from dexxy.common.logger import LoggingStuff
from dexxy.common.utils import generateUniqueID

Task = TypeVar('Task')
Pipeline = TypeVar('Pipeline')

class Task(LoggingStuff):
    
    def __init__(self, func: Callable, kwargs: dict = {}, dependsOn: List = None, skipValidation: bool = False, name: str = None, desc: str = None) -> None:
        """
        Initalization of the class Task. During inilization it will look like:
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
            skipValidation (bool, optional): If you would like to validate input/output types, this would need to be True. Defaults to False.
            name (str, optional): Name of the Task. Defaults to None.
            desc (str, optional): Description of the Task. Defaults to None.
        """
        
        self.func = func
        self.kwargs = kwargs
        self.dependsOn = dependsOn
        self.skipValidation = skipValidation
        self.name = name
        self.desc = desc
        self.status = "Not Started"
        self.related = []
        self.result = None
        self.tid = generateUniqueID()
        self._log = self.logger
        self._log.info('Built Task %s' % self.name)
        
        
    def _run(self, *args, **kwargs) -> Any:
        """
        A function that runs the func with kwargs specificed in the Task

        Returns:
            Any: If there is a df, list, etc. to return by the specific function, it will return this. 
        """
        try:
            return self.func(*args, **kwargs)
        except Exception as error:
            self._log.exception(error, exc_info=True, stack_info=True)
    
    def updateStatus(self, status: Literal['Not Started', 'Queued', 'Running', 'Completed', 'Failed'] = 'Not Started') -> None:
        """
        A function to allow updaing the status of a Task.status. It allows us to track the progress of a Task through execution. 

        Args:
            status (Literal[Not Started, Queued, Running, Completed, Failed], optional): Allows you to pass one of these options when updating the status. Defaults to 'Not Started'.
        """
        self.status = status
        
    def run(self, inputs:tuple):
        """
        Calls _run with the specified parameters where actual execution will be run. 
        """
        self.result = self._run(*inputs, **self.kwargs)
        
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
