import pandas as pd
import cx_Oracle
import sys
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
try :
 now = datetime.now()
 current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
 #con = cx_Oracle.connect("sctv_read/sctv_read@172.17.32.62:1521/gensctv")
 con = cx_Oracle.connect("SCTV_READ/sctv_read@172.17.32.37:1521/GENSCTV")
 #username/password@hostname:port/SID
 cur = con.cursor()
 print("cx_Oracle version : ", con.version)
 sql_query_niel = """SELECT * FROM TMP_UPDATE_POSTBUYNIELSEN WHERE CALC_DATE = (
    SELECT MAX(CALC_DATE) FROM TMP_UPDATE_POSTBUYNIELSEN ) ORDER BY AIRING_DATE, AIRING_TIME"""
 
 sql_query_gen = """SELECT * FROM TMP_GENSCTV WHERE CALC_DATE = (
    SELECT MAX(CALC_DATE) FROM TMP_GENSCTV) ORDER BY AIRING_DATE, AIRING_TIME"""

 df_gen = pd.read_sql(sql_query_gen, con=con)
 df_niel = pd.read_sql(sql_query_niel, con=con)
 print('The data type of df_gen is: ', type(df_gen))
 print('The data type of df_niel is: ', type(df_niel))
 print("SUCCESS TO CONNECT ORACLE")
 
 column_headers1 = df_gen.columns.values.tolist()
 print("The Column Header Gen:", column_headers1)
 
 print("=============================================")
 column_headers2 = df_niel.columns.values.tolist()
 print("The Column Header Niel:", column_headers2)

 try : 
   match_sctv = '''SELECT * FROM MATCHING_SCTV WHERE CALC_DATE    = (
      SELECT MAX(CALC_DATE) FROM MATCHING_SCTV ) ORDER BY AIRING_DATE_GEN'''
   df_match_sctv = pd.read_sql(match_sctv, con=con)

   if df_niel.loc[1,"CALC_DATE"] ==  df_match_sctv.loc[1, "CALC_DATE"]:
      with open("log_sch2.txt", "a") as f:
         print("++++++++++++++++++++++++++++++++++", file = f)
         print("there is no recent data", file = f)
         print("Calc_date Nielsen : ", df_niel.loc[1,"CALC_DATE"], file = f)
         print("Calc_date Match_SCTV : ", df_match_sctv.loc[1,"CALC_DATE"], file = f)
         print("Current Time : ", current_time , file = f)
      sys.exit("Error calc_date sama dengan input terbaru!")
   else :
      print("lanjutkan")
 except SystemExit:
   print("sys exit worked as expected")
   sys.exit()
 except :
   print("table kosong, lanjutkan")
   with open("log_sch2.txt", "a") as f:
         print("\nTable Empty! or Something trouble happen, check nohup_sctv.out for more details!", file = f)

except cx_Oracle.DatabaseError as e:
    print("There is a problem with Oracle", e)
