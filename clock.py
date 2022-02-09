from apscheduler.schedulers.background  import BlockingScheduler
from script import analysis
sched = BlockingScheduler()


def maina():
    """Run tick() at the interval of every ten seconds."""
   
    sched.add_job(analysis, 'cron', day_of_week='mon-fri', hour='3-12', minute='*/7')
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass 


maina()
