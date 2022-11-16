from typing import Tuple, Any
from uuid import uuid4, uuid5, NAMESPACE_OID

def generateUniqueID(name: str = None) -> str:
    # Comments in Tasks 32:19 into video
    
    if name:
        return str(uuid5(NAMESPACE_OID, name))
    return str(uuid4())

def getTaskResult(task) -> Tuple[Any]:
    
    inputs = []
    if task is not None:
        data = task.result
        if data is not None:
            inputs.append(data)
    return tuple(inputs)
