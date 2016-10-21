from cherry.jobs.job_tracker import execute_job
from cherry.tasks.task_tracker import task_dict
from cherry.celery import Cherry_App
from celery import chain
import json

to_slicer_in_json = {
'filters':{
	'sync_transcoder': {
		'codec': 'libx265', 
		'resolution': '1280*720',
		'framerate:': '30',
		'bitrate': '1000k'}},
'input_file_path' :'D:\\Work\\distributed_transcoder\\cherry\\video_file\\in.mp4 ',
'output_file_path':'D:\\Work\\distributed_transcoder\\cherry\\video_file\\out.mp4'
}

execute_job(to_slicer_in_str = json.dumps(to_slicer_in_json))
# print to_slicer_in_json['filters'].keys()[0]
