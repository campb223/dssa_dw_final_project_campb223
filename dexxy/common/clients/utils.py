from tasks import Task as task
from typing import Tuple, Any



def getTaskResult(task) -> Tuple[Any]:
    
    inputs = []
    if task is not None:
        data = task.result
        if data is not None:
            inputs.append(data)
    return tuple(inputs)