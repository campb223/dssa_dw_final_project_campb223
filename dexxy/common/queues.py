from queue import Queue as ThreadSafeQueue

class QueueWarehouse:
    @staticmethod
    def warehouse(type: str ='default') -> ThreadSafeQueue:
        """
        A "Queue Warehouse" that returns a queue which can be used to track the order of operation of Tasks/Pipelines. 
        
        If no type is input then the default 'ThreadSafeQueue' type will be selected. 
        
        Later additional Queue types could be added to as an elif. 
            Example. (would require import -- 'from asyncio import Queue as AsyncQueue')
                elif type == 'asyncio':
                    return AsyncQueue()  

        Args:
            type (str, optional): Defaults to 'default'.

        Raises:
            ValueError: If the 'type' is not 'default'. This is subject to change based off the information above regarding asyncio. 

        Returns:
            ThreadSafeQueue: A queue that can be used for storing the order of operation of Tasks/Pipelines/etc. 
        """
        
        if type == 'default':
            return ThreadSafeQueue()
        else:
            raise ValueError(type)