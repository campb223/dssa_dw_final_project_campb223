from typing import List, Union, TypeVar, Any, Callable, Literal
from inspect import signature
#from utils.logger import LoggingMsg
#from utils.exceptions import CompatibilityException, MissingTypeHintException
#from utils.utils import generateUUID
from abc import ABCMeta, abstractclassmethod


Task = TypeVar('Task')

class AbstractTask(metaclass=ABCMeta):
    
    @abstractclassmethod
    def run(self):
        #raise NotImplementedError('This must be implemented by a subclass.')
        raise Exception("We have an error")
    

#class BaseTask(AbstractTask, LoggingMsg): 
class BaseTask(AbstractTask): 
    def __init__(self, func: Task) -> None:
        #self.tid = generateUniqueID()
        self.func = func
    
    def __input__(self) -> List:
        annotationList = [x.annotation for x in signature(self.func).parameters.values()]
        return annotationList

    def __output__(self) -> Any:
        
        try:
            returnAnnotation = self.func.__annotations__['return']
            return returnAnnotation
        except:
            raise Exception
            #raise MissingTypeHintException(f"No type hint was provided for the {self.func.__name__}'s return")

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
        _val = any(other.__output__() is arg for arg in self.__input__())
        
        if _val is not True:
            error = f"Validation Failed. Output of {other.func.__name__,}" \
                + f"is incompatible with inputs from {self.func.__name__}"
            raise Exception(error)
            #raise CompatibilityException(error)
        else:
            return True
        
    def run(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

class Task(BaseTask):
    def __init__(
        self,
        func: Callable,
        kwargs: dict = {},
        dependOn: List = None,
        skipValidation: bool = False,
        name: str = None,
        desc: str = None) -> None:
        
        super().__init__(func=func)    
        self.kwargs = kwargs
        self.dependOn = dependOn
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
        self.result = self._run_(*inputs, **self.kwargs)
        
def createTask(inputs: Union[Task, tuple]):
    if isinstance(inputs, Task):
        return inputs
    task = Task(*inputs)
    return Task