from queue import Queue as ThreadSafeQueue

class QueueWarehouse:
    @staticmethod 
    
    # @staticmethod is a built-in decorator that defines a static method in the class in Python. 
    # A static method doesn't receive any reference argument whether it is called by an instance of a class or by the class itself.
    
    def warehouse(type: str ='default') -> ThreadSafeQueue:
        """
        A "Queue Warehouse" that returns a queue which can be used to track the order of operation of Tasks/Pipelines/Workflows. 
        
        If no type is input then the default 'ThreadSafeQueue' type will be selected. 
        
        Later additional Queue types could be added to as an elif. 
            Example. (would require import -- 'from asyncio import Queue as AsyncQueue')
                elif type == 'asyncio':
                    return AsyncQueue()  

        Args:
            type (str, optional): Defaults to 'default'. 

        Returns:
            ThreadSafeQueue: A queue that can be used for storing the order of operation of Tasks/Pipelines/etc. 
        """
        
        return ThreadSafeQueue()
