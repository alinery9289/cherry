from __future__ import absolute_import

from cherry.jobs.launch import launch_sliced_job
import time
import uuid

class jobGenerator:
    
    def __init__(self,input_file, cache_type='ftp', is_local=0):
        self.job ={
        'filters':[],
        'cache_type':cache_type,
        'is_local':is_local,
        'input_file_path': input_file,
        'authcode':'test1',
    }
    
    def getJob(self):
        return self.job

    def addOneFilter(self,out_name,next={}, codec = 'libx264', resolution= '1920x1080', bitrate='10000k', framerate='25'):
        fileid= str(uuid.uuid1()).replace('-','')
        one_filter = {'SimpleTranscoder':{
                'codec': codec, 
                'resolution': resolution,
                'bitrate': bitrate,
                'framerate:': framerate,
                'next':next,
                'output_file_path': 'D:\\Work\\distributed_transcoder\\cherry\\video_file\\'+fileid+'.'+out_name.split('.')[-1],
                'output_file_name':out_name,
                }}
        self.job['filters'].append(one_filter)

if __name__ == '__main__':
    # parameters to slicer, in json format
    input_files = [r'D:\\Work\\distributed_transcoder\\cherry\\video_file\\in.mp4']
    bitrates = ["2000k"]#"10000k","3000k",
    resolutions = ["1280x720"]#"1920x1080",,"1024x600"

    for input_file in input_files:
        job_generator = jobGenerator(input_file)
        for res in resolutions:
            for bit in bitrates:
                job_generator.addOneFilter(out_name = 'out_'+str(input_files.index(input_file))+'_'+res+'_'+bit+'.mp4',
                     resolution= res, bitrate = bit)
#         print job_generator.getJob()
        launch_sliced_job(job_generator.getJob())
        time.sleep(0.5)