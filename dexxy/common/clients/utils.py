from typing import Tuple, Any
from uuid import uuid4, uuid5, NAMESPACE_OID

def generateUniqueID(name: str = None) -> str:
    """
    Generates a unique ID
    """
    
    # If the name is provided, generate a UUID based on the SHA-1 hash of a namespace identifier (which is a UUID) and a name (which is a string).
    if name:
        return str(uuid5(NAMESPACE_OID, name))
    # Otherwise generate a random UUID.
    return str(uuid4())

def getTaskResult(task) -> Tuple[Any]:
    """
    Takes in a node in the Task with a list of UUIDs to lookup. Then looks up the data required to run a task using the provided list of UUIDs. 
    """
    
    inputs = []
    if task is not None:
        data = task.result
        if data is not None:
            inputs.append(data)
    return tuple(inputs)
