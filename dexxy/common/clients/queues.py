from asyncio import Queue as AsyncQueue
from queue import Queue as ThreadSafeQueue
from multiprocessing import JoinableQueue
from typing import Union

class QueueWarehouse:
    @staticmethod
    def warehouse(type: str ='default') -> Union[ThreadSafeQueue, AsyncQueue, JoinableQueue]:
        """
        Warehouse that returns a queue based on the provided type
        Queues 11:06 in video
        
        """
        
        if type == 'default':
            return ThreadSafeQueue()
        elif type == 'multi-threading':
            return ThreadSafeQueue()
        elif type == 'multi-processing':
            return JoinableQueue()
        elif type == 'asynchio':
            return AsyncQueue()
        else:
            raise ValueError(type)