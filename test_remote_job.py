"""
test_distributed_job.py

Test dispatch and execute job through celery.

Authors: Yanan Zhao <arthurchiao@hotmail.com>
"""

from cherry.jobs.launch import launch_quick_job
import json

if __name__ == '__main__':
    job = {
        'filters': {
            'TemplateTranscoder': {'bitrate': '400'},
        },

        # docker
        'input_file_path': '/cherry-run/video_files/in.mp4',
        'output_file_path': '/cherry-run/video_files/out.mp4'

        # linux host
        # 'input_file_path': '/home/arthurchiao/cherry/video_files/in.mp4',
        # 'output_file_path': '/home/arthurchiao/cherry/video_files/out.mp4'

        # windows
        # 'input_file_path': r'D:\\Work\\distributed_transcoder\\cherry\\video_file\\in.mp4',
        # 'output_file_path': r'D:\\Work\\distributed_transcoder\\cherry\\video_file\\out.mp4'
    }

    launch_quick_job(json.dumps(job))
