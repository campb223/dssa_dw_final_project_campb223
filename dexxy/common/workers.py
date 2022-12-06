from dexxy.common.logger import LoggingStuff
from dexxy.common.utils import getTaskResult
from dexxy.common.utils import generateUniqueID
from typing import TypeVar

Queue = TypeVar('Queue')

class Worker(LoggingStuff):

    workerID = 0
    job_id = generateUniqueID()
    
    def __init__(self, taskQueue: Queue, resultQueue: Queue):
        """
        Initalization of a Worker object. Takes in the taskQueue to know when to execute Tasks, then uses the resultsQueue to know what outputs to pass to future taskQueue executions. 

        Args:
            taskQueue (Queue): A queue of the Tasks to execute
            resultQueue (Queue): The outputs from functions called by the Tasks which could be needed for future func calls from Tasks. 
        """
        Worker.workerID += 1
        self.workerID = Worker.workerID
        self.taskQueue = taskQueue
        self.resultQueue = resultQueue
        self._log = self.logger
        self._log.info('Initalized Worker %s' % self.workerID)
    
    def start(self):
        """
        Starts execution. 
        """
        # Starts workers for processing Tasks
        self._log.info('Starting Job %s' % self.job_id)
        return self.run()
        
    def run(self): 
        """
        A loop that processes getting Tasks from the queue and processing them based on their instructions defined. 
        """

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
            
    def end(self):
        """
        Ends the execution. 
        """
        # Stops execution of Tasks
        del self.resultQueue
        