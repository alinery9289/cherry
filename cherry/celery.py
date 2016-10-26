from __future__ import absolute_import

from celery import Celery
from kombu import Queue, Exchange

from cherry.util.MyConfig import conf_dict

rabbitmq_user = conf_dict['rabbitmq']['user']
rabbitmq_password = conf_dict['rabbitmq']['password']
rabbitmq_IP = conf_dict['rabbitmq']['ip']
rabbitmq_port = conf_dict['rabbitmq']['port']

broker_str = 'amqp://' + rabbitmq_user + ':' + rabbitmq_password + '@' + rabbitmq_IP + ':' + rabbitmq_port
backend_str = 'amqp://' + rabbitmq_user + ':' + rabbitmq_password + '@' + rabbitmq_IP + ':' + rabbitmq_port
Cherry_App = Celery('Cherry',
                     broker = broker_str,
                     backend = backend_str,
                     include=['cherry.tasks.task_tracker','cherry.jobs.job_tracker'])

# Optional configuration, see the application user guide.

# Group1_route_info = {"exchange": "Task", "routing_key": "Cherry.Group1"}
# Job_route_info = {"exchange": "Job", "routing_key": "Cherry.Job"}

Cherry_App.conf.update(
    CELERY_QUEUES = (
        Queue('Cherry_Task_Group1', Exchange('Task'), routing_key='Cherry.Task.Group1'),
        Queue('Cherry_Job', Exchange('Job'), routing_key='Cherry.Job'),
    ),
    CELERY_ROUTES = {  
                     "Cherry.Task.tem_transcoder": {"exchange": "Task", "routing_key": "Cherry.Task.Group1"},                   
#                     "Cherry.Task.sync_transcoder": {"exchange": "Task", "routing_key": "Cherry.Task.Group1"},
                     "Cherry.Task.Job_tracker":{"exchange": "Job", "routing_key": "Cherry.Job"}}

    # CELERY_DEFAULT_EXCHANGE = 'Cherry.TaskGroup_1',
    # CELERY_DEFAULT_EXCHANGE_TYPE = 'topic',
    # CELERY_DEFAULT_ROUTING_KEY = 'Cherry.TaskGroup_1.#',
)

if __name__ == '__main__':
    Cherry_App.start()
