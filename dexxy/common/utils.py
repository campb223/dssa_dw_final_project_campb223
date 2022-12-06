from typing import Tuple, Any
from uuid import uuid4, uuid5, NAMESPACE_OID

def generateUniqueID(name: str = None) -> str:
    """
    Generates a unique ID based off input (if any). Additional documentation relted to uuid4/uuid5 can be found at:
        https://docs.python.org/3/library/uuid.html

    Args:
        name (str) [optional]: Defaults to None and assumes we're safe to generate a UUID4/UUID5. 

    Returns:
        str: 
            UUID4 -- Generate a random UUID.
            UUID5 -- Generate a UUID based on the SHA-1 hash of a namespace identifier (which is a UUID) and a name (which is a string).
    """
 
    # If the name is provided, generate a UUID based on the SHA-1 hash of a namespace identifier and a name. 
    if name:
        return str(uuid5(NAMESPACE_OID, name))
    # Otherwise generate a random UUID.
    return str(uuid4())