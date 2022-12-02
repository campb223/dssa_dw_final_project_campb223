from typing import List, Union, TypeVar, Any, Callable, Literal, Dict, Tuple
from inspect import signature
from dexxy.common.logger import LoggingStuff
from dexxy.common.exceptions import CompatibilityException, MissingTypeHintException
from dexxy.common.utils import generateUniqueID

Task = TypeVar('Task')
Pipeline = TypeVar('Pipeline')


class Task(LoggingStuff):
    
    def __init__( 
            self,
            func: Callable,
            kwargs: dict = {},
            dependsOn: List = None,
            skipValidation: bool = False,
            name: str = None,
            desc: str = None) -> None:
        
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
        
    def _run(self, *args, **kwargs) -> Any:
        try:
            return self.func(*args, **kwargs)
        except Exception as error:
            self._log.exception(error, exc_info=True, stack_info=True)
    
    def updateStatus(self, status: Literal['Not Started', 'Queued', 'Running', 'Completed', 'Failed'] = 'Not Started') -> None:
        self.status = status
        
    def run(self, inputs:tuple):
        self.result = self._run(*inputs, **self.kwargs)
        
def createTask(inputs: Union[Task, tuple]):
    if isinstance(inputs, Task):
        return inputs
    elif isinstance(inputs, Tuple):
        return Task(*inputs)
    else:
        raise TypeError('Step must be a Task, Pipeline, or Tuple.')
