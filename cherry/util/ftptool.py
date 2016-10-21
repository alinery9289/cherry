#coding=utf-8
from __future__ import absolute_import
from ftplib import FTP
from ftplib import ftpcp
import os,sys,string,datetime,time
import socket
# import cherry.util.MyConfig 
from cherry.util.MyConfig import conf_dict

class MYFTP:
    def __init__(self, hostaddr, username, password, remotedir, port=21):
        self.hostaddr = hostaddr
        self.username = username
        self.password = password
        self.remotedir  = remotedir
        self.port     = port
        self.ftp      = FTP()
        self.file_list = []
        # self.ftp.set_debuglevel(2)
    def __del__(self):
        self.ftp.close()
        # self.ftp.set_debuglevel(0)
    def login(self):
        ftp = self.ftp
        try: 
            timeout = 300
            socket.setdefaulttimeout(timeout)
            ftp.set_pasv(True)
            #print u'Connecting %s' %(self.hostaddr)
            ftp.connect(self.hostaddr, self.port)
            #print u'%s Connected' %(self.hostaddr)
            #print u'Begin to login %s' %(self.hostaddr)
            ftp.login(self.username, self.password)
            #print u'Login %s succeed' %(self.hostaddr)
            debug_print(ftp.getwelcome())
        except Exception:
            print u'Login failed'
        try:
            ftp.cwd(self.remotedir)
        except(Exception):
            print u'Change dir failed'
        
    def download_file(self, localfile, remotefile):
        file_handler = open(localfile, 'wb')
        self.ftp.retrbinary(u'RETR %s'%(remotefile), file_handler.write)
        file_handler.close()
        
    def upload_file(self, localfile, remotefile):
        if not os.path.isfile(localfile):
            return
        if self.is_same_size(localfile, remotefile):
            debug_print(u'Skip %s' %localfile)
            return
        file_handler = open(localfile, 'rb')
        
        remotedir=remotefile.split('/')
        for now_dir in remotedir[:-1]:
            try:
                self.ftp.mkd(now_dir)
            except:
                pass
            self.ftp.cwd(now_dir)
        self.ftp.storbinary('STOR %s' %remotefile, file_handler)
        file_handler.close()
        debug_print(u'Have delivered %s' %localfile)


    def get_file_list(self, line):
        ret_arr = []
        file_arr = self.get_filename(line)
        if file_arr[1] not in ['.', '..']:
            self.file_list.append(file_arr)
            
    def get_filename(self, line):
        pos = line.rfind(':')
        while(line[pos] != ' '):
            pos += 1
        while(line[pos] == ' '):
            pos += 1
        file_arr = [line[0], line[pos:]]
        return file_arr
    
def debug_print(s):
    print s
    
            
        
#     def is_same_size(self, localfile, remotefile):
#         try:
#             remotefile_size = self.ftp.size(remotefile)
#         except:
#             remotefile_size = -1
#         try:
#             localfile_size = os.path.getsize(localfile)
#         except:
#             localfile_size = -1
#         debug_print('localfile_size:%d  remotefile_size:%d' %(localfile_size, remotefile_size),)
#         if remotefile_size == localfile_size:
#             return 1
#         else:
#             return 0
  
#     def download_files(self, localdir='./', remotedir='./'):
#         try:
#             self.ftp.cwd(remotedir)
#         except:
#             debug_print(u'Dir %s is not existed，Continue...' %remotedir)
#             return
#         if not os.path.isdir(localdir):
#             os.makedirs(localdir)
#         debug_print(u'Change to dir %s' %self.ftp.pwd())
#         self.file_list = []
#         self.ftp.dir(self.get_file_list)
#         remotenames = self.file_list
#         #print(remotenames)
#         #return
#         for item in remotenames:
#             filetype = item[0]
#             filename = item[1]
#             local = os.path.join(localdir, filename)
#             if filetype == 'd':
#                 self.download_files(local, filename)
#             elif filetype == '-':
#                 self.download_file(local, filename)
#         self.ftp.cwd('..')
#         debug_print(u'Back to dir %s' %self.ftp.pwd())      

#     def upload_files(self, localdir='./', remotedir = './'):
#         if not os.path.isdir(localdir):
#             return
#         localnames = os.listdir(localdir)
#         self.ftp.cwd(remotedir)
#         for item in localnames:
#             src = os.path.join(localdir, item)
#             if os.path.isdir(src):
#                 try:
#                     self.ftp.mkd(item)
#                 except:
#                     debug_print(u'dir is existed: %s' %item)
#                 self.upload_files(src, item)
#             else:
#                 self.upload_file(src, item)
#         self.ftp.cwd('..')

hostaddr = conf_dict['ftp']['hostaddr']
username = conf_dict['ftp']['username']
password = conf_dict['ftp']['password']
port  =  conf_dict['ftp']['port']
rootdir_local  = conf_dict['ftp']['rootdir_local']
rootdir_remote = conf_dict['ftp']['rootdir_remote']
     
f = MYFTP(hostaddr, username, password, rootdir_remote, port)

def upload_file(localfile, remotefile):
    f.login()    
    f.upload_file(''.join([rootdir_local,localfile]), ''.join([rootdir_remote,remotefile]))

def get_file(localfile, remotefile):
    f.login()    
    f.download_file(''.join([rootdir_local,localfile]), ''.join([rootdir_remote,remotefile]))
