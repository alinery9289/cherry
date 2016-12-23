from cherry.jobs.launch import launch_sliced_job

if __name__ == '__main__':
    # parameters to slicer, in json format
    job = {
        'filters':[{'SimpleTranscoder':{
                'codec': 'libx265', 
                'resolution': '1280*720',
                'framerate:': '30',
                'bitrate': '1000k',
                'next':[{'TemplateTranscoder': {'bitrate': '2000','next':{}, 'output_file_path': r'D:\Project\workpath\video_file\out3.mp4' }},           
                        {'TemplateTranscoder': {'bitrate': '1500','next':{}, 'output_file_path': r'D:\Project\workpath\video_file\out4.mp4'}}] }}],
        'cache_type':'ftp',
        'is_local':0,
        # linux
        # 'input_file_path': '/home/cloud/cherry/video_files/in.mp4',
        # 'output_file_path': '/home/cloud/cherry/video_files/out.mp4'

        # windows
        'input_file_path': r'D:\Project\workpath\video_file\in.mp4',
    }

    launch_sliced_job(job)

