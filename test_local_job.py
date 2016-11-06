from cherry.jobs.launch import launch_normal_job
import json

if __name__ == '__main__':
    # parameters to slicer, in json format
    job = {
        'filters': {
            # 	'SyncTranscoder': {'codec': 'libx265', 'resolution': '1280*720','framerate:': '30','bitrate': '1000k',
            # 		'next':{'blank_transcoder':{}},},
            'TemplateTranscoder': {'bitrate': '400'},

        },
        # linux
#         'input_file_path': '/cherry-run/video_files/in.mp4',
#         'output_file_path': '/cherry-run/video_files/out.mp4'
        # 'input_file_path': '/home/cloud/cherry/video_files/in.mp4',
        # 'output_file_path': '/home/cloud/cherry/video_files/out.mp4'

        # windows
        'input_file_path': r'D:\\Work\\distributed_transcoder\\cherry\\video_file\\in.mp4',
        'output_file_path': r'D:\\Work\\distributed_transcoder\\cherry\\video_file\\out.mp4'
    }

    launch_normal_job(job)
