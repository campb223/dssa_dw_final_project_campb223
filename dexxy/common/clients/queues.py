from asyncio import Queue as AsyncQueue
from queue import Queue as Queue
from multiprocessing import JoinableQueue
from typing import Union

class QueueWarehouse:
    @staticmethod
    def warehouse(type: str ='default') -> Union[Queue, AsyncQueue, JoinableQueue]:
        """
        Warehouse that returns a queue based on the provided type
        
        
        """
        
        if type == 'default':
            return Queue()
        elif type == 'multi-threading':
            return Queue()
        elif type == 'multi-processing':
            return JoinableQueue()
        elif type == 'asynchio':
            return AsyncQueue()
        else:
            raise ValueError(type)