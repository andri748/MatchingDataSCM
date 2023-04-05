
import schedule
import time
from datetime import datetime
import os

def app_run():
 try :
   now = datetime.now()
   print("RUNNING")
   current_time = now.strftime("%H:%M:%S")
   print("Current Time =", current_time)
   print("here we go..")
   os.system ('python3 app2.py')
   if current_time >= "14:00:00":
      print("END BACK TO SCHEDULE")
      schedule.clear('minute_task')
      
 except :
   print("error!")
   with open("log_sch2.txt", "a") as f:
      print("error in app2.py or connect2.py!!", file = f)
   if current_time >= "14:00:00":
      print("END BACK TO SCHEDULE")
      schedule.clear('minute_task')
      
   pass

def startapp():
 
 print("Enter to Minutes")
 schedule.every(15).minutes.do(app_run).tag('minute_task')

print("WAIT SCHEDULE")
schedule.every().day.at("04:00").do(startapp)

# Loop so that the scheduling task
# keeps on running all time.
while True:
 
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    time.sleep(1)
