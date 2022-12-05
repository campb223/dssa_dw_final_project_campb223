# Defining some exceptions we could expect to see during runtime.

class CircularDependencyError(Exception):
    pass

class MissingDependencyError(Exception):
    pass

class DependencyError(Exception):
    pass

class NotFoundError(Exception):
    pass