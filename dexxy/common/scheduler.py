from apscheduler.schedulers.background import BackgroundScheduler


class Scheduler(BackgroundScheduler):
    """Implements a Singleton Design Pattern for BackgroundScheduler
    The BackgroundScheduler runs a workflow in a seperate thread
    """

    def __new__(cls):
        return super(Scheduler, cls).__new__(cls)