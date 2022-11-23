from abc import abstractclassmethod, ABCMeta
from dexxy.common.clients.utils import generateUniqueID
from dexxy.common.clients.logger import LoggingStuff
from typing import TypeVar


Queue = TypeVar('Queue')

class AbstractBaseExecutor(metaclass=ABCMeta):
    
    @abstractclassmethod
    def start(self):
        return NotImplementedError('Abstract Method that must be called by the subclass')
    
    @abstractclassmethod
    def stop(self):
        return NotImplementedError('Abstract Method that must be called by the subclass')
    
    
class BaseExecutor(AbstractBaseExecutor, LoggingStuff):
    
    job_id = generateUniqueID()
    
    def __init__(self, taskQueue:Queue, resultQueue:Queue):
        super().__init__()
        self.taskQueue = taskQueue
        self.resultQueue = resultQueue
        self._log = self.logger
        
    def start(self):
        # Starts workers for processing Tasks
        return
    
    def stop(self):
        # Stops execution of Tasks
        return