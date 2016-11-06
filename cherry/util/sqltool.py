from sqlalchemy import create_engine
from cherry.util.config import conf_dict
from sqlalchemy.sql.sqltypes import BigInteger

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, INTEGER, CHAR, DATETIME

db_username = conf_dict['mysql']['username']
db_password = conf_dict['mysql']['password']
db_host = conf_dict['mysql']['ip']
db_db_name = conf_dict['mysql']['db']

engine_info = "mysql+mysqldb://" + db_username + ":" + db_password + "@" + \
    db_host + "/" + db_db_name
engine = create_engine(engine_info)

Base = declarative_base()


class User(Base):
    __tablename__ = 'h264tohevc_user'
    authcode = Column(CHAR, primary_key=True, unique=True)
    email = Column(CHAR)
    userid = Column(CHAR)
    password = Column(CHAR)
    userstorage = Column(BigInteger)
    secretkey = Column(CHAR)
    outdate = Column(DATETIME)


class MediaFile(Base):
    __tablename__ = 'h264tohevc_mediafile'
    fileid = Column(CHAR, primary_key=True, unique=True)
    filename = Column(CHAR)
    authcode = Column(CHAR)
    filesize = Column(BigInteger)
    location = Column(CHAR)
    filetype = Column(CHAR)
    md5 = Column(CHAR)
    uploadtime = Column(DATETIME)
    encodeinfo = Column(CHAR)

    def __repr__(self):
        return "<mediafile(fileid='%s', filename='%s', authcode='%s')>" % (
            self.fileid, self.filename, self.authcode)


class WebTask(Base):
    __tablename__ = 'h264tohevc_processlog'
    taskid = Column(CHAR, primary_key=True, unique=True)
    fileid = Column(CHAR)
    dealmethod = Column(CHAR)
    controljson = Column(CHAR)
    dealstate = Column(CHAR)
    afterfileid = Column(CHAR)

    dealtime = Column(DATETIME)
    completetime = Column(DATETIME)

    def __repr__(self):
        return "<task(taskid='%s', dealtime='%s', dealstate='%s')>" % (
            self.taskid, self.dealtime, self.dealstate)


class TaskGroup(Base):
    __tablename__ = 'h264tohevc_taskgroup'
    groupid = Column(INTEGER, primary_key=True, unique=True, autoincrement=True)
    tasklist = Column(CHAR)

    def __repr__(self):
        return "<taskgroup(groupid='%s', tasklist='%s'')>" % (
            self.groupid, self.tasklist)

#     CELERY_ROUTES = {"Cherry.Task.task_B": {
#                          "exchange": "Task",
#                          "routing_key": "Cherry.Group2"},
#                     "Cherry.Task.task_A": {
#                          "exchange": "Task",
#                          "routing_key": "Cherry.Group1"}}

from sqlalchemy.orm import sessionmaker


# def get_CELERY_ROUTES():
# 
#     # Base.metadata.create_all(engine)
# 
#     Session = sessionmaker(bind=engine)
#     session = Session()
#     CELERY_ROUTES = {}
#     for instance in session.query(TaskGroup):
#         tasks = str(instance.tasklist).split(',')
#         for task in tasks:
#             one_task_message = {}
#             one_task_message[
#                 "routing_key"] = "Cherry.Group"+str(instance.groupid)
#             one_task_message["exchange"] = "Task"
#             CELERY_ROUTES["Cherry.Task.task_"+task] = one_task_message
# 
#     return CELERY_ROUTES


# if __name__ == '__main__':
#     import datetime
#     Session = sessionmaker(bind=engine)
#     session = Session()
# 
#     for instance in session.query(MediaFile):
#         print instance
#     oneusers = session.query(User).filter(
#         User.authcode == "38a43f8070e811e5ad0c90b11c94ab4d")
#     for oneuser in oneusers:
#         now_storage = oneuser.userstorage - 2000
#     oneusers.update({User.userstorage: now_storage})
#     session.commit()
#     session.close()
