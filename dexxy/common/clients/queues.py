from queue import Queue as ThreadSafeQueue

class QueueWarehouse:
    @staticmethod
    def warehouse(type: str ='default') -> ThreadSafeQueue:
        """
        A "Queue Warehouse" that returns a queue which can be used to execute Tasks in the order needed.        
        """
        
        if type == 'default':
            return ThreadSafeQueue()
        else:
            raise ValueError(type)