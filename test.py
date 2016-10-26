from cherry.jobs.job_tracker import execute_quick_job, execute_normal_job
import json

# parameters to slicer, in json format
params = {
    'filters': {
        # 	'sync_transcoder': {'codec': 'libx265', 'resolution': '1280*720','framerate:': '30','bitrate': '1000k',
        # 		'next':{'blank_transcoder':{}},},
        'tem_transcoder': {'bitrate': '4000'},

    },
    'input_file_path': r'D:\\Work\\distributed_transcoder\\cherry\\video_file\\in.mp4',
    'output_file_path': r'D:\\Work\\distributed_transcoder\\cherry\\video_file\\out.mp4'
}

execute_normal_job(params)
# print to_slicer_in_json['filters'].keys()[0]
