from dexxy.common.logger import LoggingStuff
from dexxy.common.utils import getTaskResult
from dexxy.common.utils import generateUniqueID
from typing import TypeVar

Queue = TypeVar('Queue')

class DefaultWorker(LoggingStuff):
    """_summary_

    Args:
        LoggingStuff (_type_): _description_
    """
    workerID = 0
    
    def __init__(self, taskQueue: Queue, resultQueue: Queue):
        """_summary_

        Args:
            taskQueue (Queue): _description_
            resultQueue (Queue): _description_
        """
        DefaultWorker.workerID += 1
        self.workerID += 1
        self.taskQueue = taskQueue
        self.resultQueue = resultQueue
        self._log = self.logger
        
    def run(self): 

        while not self.taskQueue.empty():
            
            _task = self.taskQueue.get()
            _task.updateStatus('Running')
            self._log.info('Running Tasks %s on Worker %s ' % (_task.name, self.workerID))
            
            if _task.dependsOn:
                inputs = ()
                for depTask in list(dict.fromkeys(_task.dependsOn).keys()):
                    for completedTask in list(self.resultQueue.queue):
                        if depTask.tid == completedTask.tid:
                            inputData = getTaskResult(completedTask)
                            inputs = inputs + inputData
            else:
                inputs = tuple()
                
            _task.run(inputs)
            _task.updateStatus('Completed')
            
            self.resultQueue.put(_task)
            
            self.taskQueue.task_done()
            
class DefaultExecutor(LoggingStuff):
    """_summary_

    Args:
        LoggingStuff (_type_): _description_
    """
    
    job_id = generateUniqueID()
    
    def __init__(self, taskQueue, resultQueue):
        """_summary_

        Args:
            taskQueue (_type_): _description_
            resultQueue (_type_): _description_
        """
        self.taskQueue = taskQueue
        self.resultQueue = resultQueue
        self._log = self.logger
        
    def start(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        # Starts workers for processing Tasks
        self._log.info('Starting Job %s' % self.job_id)
        self.worker = DefaultWorker(self.taskQueue, self.resultQueue)
        return self.worker.run()
        
    def end(self):
        """_summary_
        """
        # Stops execution of Tasks
        del self.worker
        #del self.resultQueue