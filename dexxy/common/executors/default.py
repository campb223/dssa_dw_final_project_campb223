from dexxy.common.executors.base import BaseExecutor
from dexxy.common.clients.logger import LoggingStuff
from dexxy.common.clients.utils import getTaskResult
from typing import TypeVar

Queue = TypeVar('Queue')

class DefaultWorker(LoggingStuff):
    workerID = 0
    
    def __init__(self, taskQueue: Queue, resultQueue: Queue):
        DefaultWorker.workerID += 1
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
            
class DefaultExecutor(BaseExecutor):
    def __init__(self, taskQueue, resultQueue):
        super().__init__(taskQueue, resultQueue)
        
    def start(self):
        self._log.info('Starting Job %s' % self.job_id)
        self.worker = DefaultWorker(self.taskQueue, self.resultQueue)
        return self.worker.run()
        
    def end(self):
        del self.worker
        #del self.resultQueue