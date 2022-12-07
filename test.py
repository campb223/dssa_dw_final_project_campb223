To use the DefaultScheduler class to schedule the execution of pipelines, you would first need to create an instance of the DefaultScheduler class. Then, you can use the add_job method of the scheduler to add a pipeline to the scheduler's queue. When you add a pipeline to the queue, you can specify the time and frequency at which the pipeline should be executed, as well as any additional arguments or options for the pipeline. For example, you could use the following code to add a pipeline to the scheduler's queue:

Copy code
# Create an instance of the DefaultScheduler
scheduler = DefaultScheduler()

# Add a pipeline to the scheduler's queue to be executed at a specific time
scheduler.add_job(pipeline, 'date', run_date=datetime(2022, 12, 7, 12, 0, 0))

# Start the scheduler
scheduler.start()
Alternatively, you could use the add_cron_job method to add a pipeline to the scheduler's queue to be executed at regular intervals. For example, you could use the following code to add a pipeline to the scheduler's queue to be executed every hour:

Copy code
# Create an instance of the DefaultScheduler
scheduler = DefaultScheduler()

# Add a pipeline to the scheduler's queue to be executed every hour
scheduler.add_cron_job(pipeline, 'hourly')

# Start the scheduler
scheduler.start()
Once a pipeline has been added to the scheduler's queue, the scheduler will execute the pipeline at the specified time or interval. The pipeline will be executed in a separate thread, so it will not block the main thread of execution.