from sqlalchemy import create_engine
engine=create_engine("mysql+mysqldb://root:medialab_513@202.120.39.226/test")

# conn=MySQLdb.connect(host="202.120.39.226",user="root",passwd="medialab_513",db="test",charset="utf8")

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, INTEGER, CHAR, DATETIME

Base = declarative_base()

class Web_Task(Base):
	__tablename__ = 'h264tohevc_processlog' 
	# id = Column(Integer, primary_key=True, unique=True)
	# name = Column(String)
	# fullname = Column(String)
	# password = Column(String)  

	taskid = Column(CHAR, primary_key=True, unique=True)
	fileid = Column(CHAR)
	dealmethod =  Column(CHAR)
	controljson =  Column(CHAR)
	dealstate =  Column(CHAR)
	afterfileid = Column(CHAR)

	dealtime = Column(DATETIME)
	completetime = Column(DATETIME)


	def __repr__(self):
		return "<task(taskid='%s', dealtime='%s', dealstate='%s')>" % (self.taskid, self.dealtime, self.dealstate)

from sqlalchemy.orm import sessionmaker

if __name__ == '__main__':
	
	# 写数据库
	# Base.metadata.create_all(engine)

	Session = sessionmaker(bind=engine)
	session = Session()

	for instance in session.query(Web_Task).filter_by(dealstate='raw').order_by(Web_Task.taskid): 
		print instance


