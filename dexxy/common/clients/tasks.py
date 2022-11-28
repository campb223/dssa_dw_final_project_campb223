from typing import List, Union, TypeVar, Any, Callable, Literal, Dict, Tuple
from inspect import signature
from dexxy.common.clients.logger import LoggingStuff
from dexxy.common.clients.exceptions import CompatibilityException, MissingTypeHintException
from dexxy.common.clients.utils import generateUniqueID
from abc import ABCMeta, abstractclassmethod


Task = TypeVar('Task')
Pipeline = TypeVar('Pipeline')


class AbstractTask(metaclass=ABCMeta):
    """Abstract class of Task that must be implemented by the BaseTask class

    Raises:
        NotImplementedError: If this class isn't implemented by the BaseTask Class - throw an error
    """
    
    @abstractclassmethod
    def validate(self):
        raise NotImplementedError('This must be implemented by a subclass.')
    
    @abstractclassmethod
    def run(self):
        
        raise NotImplementedError('This must be implemented by a subclass.')
    

class BaseTask(AbstractTask, LoggingStuff):
    """_summary_

    Args:
        AbstractTask (_type_): _description_
        LoggingMsg (_type_): _description_
    """
    
    def __init__(self, func: Callable) -> None:
        super().__init__()
        self.tid = generateUniqueID()
        self.func = func
        self._log = self.logger
    
    def __input__(self) -> List:
        """
        Parse the arguments of func to a list of allowed types. 
        """
        
        annotationList = [x.annotation for x in signature(self.func).parameters.values()]
        return annotationList

    def __output__(self) -> Any:
        """
        Parse the Return type from func
        
        Returns:
            returnAnnotation : type annotation for the return statement of func
        """
        
        try:
            returnAnnotation = self.func.__annotations__['return']
            return returnAnnotation
        except:
            raise MissingTypeHintException(f"No type hint was provided for the {self.func.__name__}'s return")

    def __str__(self) -> str:
        """
        #Basically just returns the way to print this with print(x) is called
        """
        from pprint import pprint
        s = dict()
        s['Task'] = self.__dict__.copy()
        s['Task']['input'] = self.__input__()
        s['Task']['output'] = self.__output__()
        return str(pprint(s))
        
    def __repr__(self) -> str:
        items = self.__dict__.copy()
        items['input'] = self.__input__()
        items['output'] = self.__output__()
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join('{}={!r}'.format(k,v) for k, v in items.items())
        )
    
    def validate(self, other: Task) -> bool:
        # Comments in Building Tasks 27:05 into vid
        _val = any(other.__output__() is arg for arg in self.__input__())
        
        if other.__output__() is Any:
            error = f"Cannot check compatibility with previous task {other.func.__name__} when return type is 'Any'"
            raise CompatibilityException(error)
        
        if _val is not True:
            error = f"Validation Failed. Output of {other.func.__name__,}" + f"is incompatible with inputs from {self.func.__name__}"
            raise CompatibilityException(error)
        else:
            return True
        
    def _run(self, *args, **kwargs) -> Any:
        try:
            return self.func(*args, **kwargs)
        except Exception as error:
            self._log.exception(error, exc_info=True, stack_info=True)
 
 
            
class Task(BaseTask):
    def __init__(
            self,
            func: Callable,
            kwargs: dict = {},
            dependsOn: List = None,
            skipValidation: bool = False,
            name: str = None,
            desc: str = None) -> None:
        
        super().__init__(func=func)    
        self.kwargs = kwargs
        self.dependsOn = dependsOn
        self.skipValidation = skipValidation
        self.name = name
        self.desc = desc
        self.status = "Not Started"
        self.related = []
        self.result = None

    def __str__(self) -> str:
        from pprint import pprint
        s = dict()
        s['Activity'] = self.__dict__.copy()
        return str(pprint(s))
    
    def __repr__(self) -> str:
        return "<class '{}({})>'".format(
            self.__class__.__name__,
            ''.join('{}={!r},'.format(k,v) for k,v in self.__dict__.items())
        )
        
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