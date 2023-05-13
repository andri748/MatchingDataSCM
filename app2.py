import cx_Oracle
import connect2
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import time
from datetime import datetime
start = time.time()


"""###LOAD DATASET"""

gn = connect2.df_gen.rename(columns={"AIRING_DATE":"AIRING_DATE_GEN","AIRING_TIME":"AIRING_TIME_GEN","PROGRAM_NAME":"PROGRAM_NAME_GEN", 
"PRODUCT_NAME":"PRODUCT_NAME_GEN","PROD_VERSION_NAME": "PROD_VERSION_NAME_GEN","PROD_GROUP":"PROD_GROUP_GEN"})
ni = connect2.df_niel.rename(columns={"AIRING_DATE":"AIRING_DATE_NIEL","AIRING_TIME":"AIRING_TIME_NIEL","PROGRAM_TITLE":"PROGRAM_NAME_NIEL",
"PRODUCT_NAME":"PRODUCT_NAME_NIEL","PROD_VERSION":"PROD_VERSION_NAME_NIEL","PROD_GROUP":"PROD_GROUP_NIEL"})


gn1 = gn.get(["AIRING_DATE_GEN","AIRING_TIME_GEN","PROGRAM_NAME_GEN","PRODUCT_NAME_GEN","PROD_VERSION_NAME_GEN"
,"MO_NO","PO_TYPE","PROD_CODE","PROD_VERSION","ROW_ID_SLOT","ROW_ID_SPOT","PROD_GROUP_GEN"])

ni1 = ni.get(["AIRING_DATE_NIEL","AIRING_TIME_NIEL", "PROGRAM_NAME_NIEL","PRODUCT_NAME_NIEL", "PROD_GROUP_NIEL","RID_NIELSEN","CALC_DATE"])

print("shape gen : ", gn1.shape)
print("Shape Niel : ", ni1.shape)
gn1['MERGE'] = 1
ni1['MERGE'] = 1

#merge dengan cara memecah jam
#memecah gen menjadi 4 bagian waktu
gen_0to8 = pd.DataFrame()
gen_8to16 = pd.DataFrame()
gen_16to24 = pd.DataFrame()
gen_24to30 = pd.DataFrame()

for i in range(len(gn1['AIRING_TIME_GEN'])):
   if '00000000' <= gn1.loc[i, 'AIRING_TIME_GEN'] <= '08000000' :
    gen_0to8 = gen_0to8.append(gn1.loc[i], ignore_index=True)
   elif '07550000' <= gn1.loc[i, 'AIRING_TIME_GEN'] <= '16000000' :
    gen_8to16 = gen_8to16.append(gn1.loc[i], ignore_index=True)
   elif '15550000' <= gn1.loc[i, 'AIRING_TIME_GEN'] <= '24000000' :
    gen_16to24 = gen_16to24.append(gn1.loc[i], ignore_index=True)
   elif '23550000' <= gn1.loc[i, 'AIRING_TIME_GEN'] <= '30000000' :
    gen_24to30 = gen_24to30.append(gn1.loc[i], ignore_index=True)
print("=========================================")
jumlah_gen = len(gen_0to8) + len(gen_8to16) + len(gen_16to24) + len(gen_24to30)
print("jumlah gen: ", jumlah_gen)

#memecah niel menjadi 4 bagian waktu
niel_0to8 = pd.DataFrame()
niel_8to16 = pd.DataFrame()
niel_16to24 = pd.DataFrame()
niel_24to30 = pd.DataFrame()

for i in range(len(ni1['AIRING_TIME_NIEL'])):
   if '00000000' <= ni1.loc[i, 'AIRING_TIME_NIEL'] <= '08000000' :
    niel_0to8 = niel_0to8.append(ni1.loc[i], ignore_index=True)
   elif '07550000' <= ni1.loc[i, 'AIRING_TIME_NIEL'] <= '16000000' :
    niel_8to16 = niel_8to16.append(ni1.loc[i], ignore_index=True)
   elif '15550000' <= ni1.loc[i, 'AIRING_TIME_NIEL'] <= '24000000' :
    niel_16to24 = niel_16to24.append(ni1.loc[i], ignore_index=True)
   elif '23550000' <= ni1.loc[i, 'AIRING_TIME_NIEL'] <= '30000000' :
    niel_24to30 = niel_24to30.append(ni1.loc[i], ignore_index=True)
print("=========================================")
jumlah_niel = len(niel_0to8) + len(niel_8to16) + len(niel_16to24) + len(niel_24to30)
print("jumlah niel : ", jumlah_niel)

#append berdasarkan waktu
gen_0to8 = gen_0to8.append(gen_24to30, ignore_index = True)
niel_0to8 = niel_0to8.append(niel_24to30, ignore_index = True)
#merge berdasarkan waktu 
df_0to8 = gen_0to8.merge(niel_0to8, how = 'inner')
df_8to16 = gen_8to16.merge(niel_8to16,how='inner')
df_16to24 = gen_16to24.merge(niel_16to24,how='inner')
#append semuanya
df0 = df_0to8.append(df_8to16)
df = df0.append(df_16to24)
print("=========================================")
print("Dataset Shape :", df.shape)
print("DATA 04.30 to 23.59 SUCCESS merge")


"""###RAPIHIN DATA"""
df = df.drop(["MERGE"], axis=1)
ardate2 = df['AIRING_DATE_NIEL']
df.drop(labels=['AIRING_DATE_NIEL'], axis=1,inplace = True)
df.insert(1, 'AIRING_DATE_NIEL', ardate2)

artigen2 = df['AIRING_TIME_GEN']
df.drop(labels=['AIRING_TIME_GEN'], axis=1,inplace = True)
df.insert(2, 'AIRING_TIME_GEN', artigen2)

artiniel2 = df['AIRING_TIME_NIEL']
df.drop(labels=['AIRING_TIME_NIEL'], axis=1,inplace = True)
df.insert(3, 'AIRING_TIME_NIEL', artiniel2)

pnn2 = df['PROGRAM_NAME_NIEL']
df.drop(labels=['PROGRAM_NAME_NIEL'], axis=1,inplace = True)
df.insert(4, 'PROGRAM_NAME_NIEL', pnn2)


png2= df['PROGRAM_NAME_GEN']
df.drop(labels=['PROGRAM_NAME_GEN'], axis=1,inplace = True)
df.insert(5, 'PROGRAM_NAME_GEN', png2)

po_type2 = df['PO_TYPE']
df.drop(labels=['PO_TYPE'], axis=1,inplace = True)
df.insert(9, 'PO_TYPE', po_type2)

prodnn2 = df['PRODUCT_NAME_NIEL']
df.drop(labels=['PRODUCT_NAME_NIEL'], axis=1,inplace = True)
df.insert(7, 'PRODUCT_NAME_NIEL', prodnn2)

pg2 = df['PROD_GROUP_NIEL']
df.drop(labels=['PROD_GROUP_NIEL'], axis=1,inplace = True)
df.insert(8, 'PROD_GROUP_NIEL', pg2)

pg3 = df['PROD_GROUP_GEN']
df.drop(labels=['PROD_GROUP_GEN'], axis=1,inplace = True)
df.insert(9, 'PROD_GROUP_GEN', pg3)


"""#FILTERING
##Data Processing
"""

dataSCM = pd.DataFrame()
data_sort = df.sort_values(by=['AIRING_TIME_NIEL'], ascending = False)
dataSCM = dataSCM.append(data_sort, ignore_index=True)

hasil0 = pd.DataFrame()
for i in range(len(dataSCM["PROGRAM_NAME_NIEL"])):
 if dataSCM.loc[i,"AIRING_TIME_GEN"] <= '04300000':
  conv_gen = int(dataSCM.loc[i,'AIRING_TIME_GEN']) + 24000000
  hasil = int(dataSCM.loc[i,'AIRING_TIME_NIEL']) - conv_gen  
 else : 
  hasil = int(dataSCM.loc[i,'AIRING_TIME_NIEL']) - int(dataSCM.loc[i,'AIRING_TIME_GEN']) 
 if hasil <= 1000 :     
  if hasil >= -1000 :
     sel1 = dataSCM.loc[i]
     hasil0 = hasil0.append(sel1, ignore_index=True)
hasil0akur  = pd.DataFrame()
  
from fuzzywuzzy import fuzz
#kemiripan
def getSimularityPartialScore(str1,str2):
    return fuzz.token_set_ratio(str1.lower(), str2.lower())    

for i in range(len(hasil0['PRODUCT_NAME_GEN'])):
 a = str(hasil0.loc[i, 'PRODUCT_NAME_GEN'])
 b = str(hasil0.loc[i, 'PRODUCT_NAME_NIEL'])
 ratio =  getSimularityPartialScore(a,b)
 if ratio >= 65 :
   ratio1 = hasil0.loc[i]
   hasil0akur = hasil0akur.append(ratio1, ignore_index=True)
 else :
  continue

#hasil0akur  = pd.DataFrame()
  
for i in range(len(hasil0['PRODUCT_NAME_GEN'])):
 a = str(hasil0.loc[i, 'PRODUCT_NAME_GEN'])
 b = str(hasil0.loc[i, 'PROD_GROUP_NIEL'])
 ratio =  getSimularityPartialScore(a,b)
 if ratio >= 65:
   ratio1 = hasil0.loc[i]
   hasil0akur = hasil0akur.append(ratio1, ignore_index=True)
 else :
  continue

for i in range(len(hasil0['PRODUCT_NAME_GEN'])):
 a = str(hasil0.loc[i, 'PRODUCT_NAME_NIEL'])
 b = str(hasil0.loc[i, 'PROD_GROUP_GEN'])
 ratio =  getSimularityPartialScore(a,b)
 if ratio >= 65:
   ratio1 = hasil0.loc[i]
   hasil0akur = hasil0akur.append(ratio1, ignore_index=True)
 else :
  continue

for i in range(len(hasil0['PROD_GROUP_GEN'])):
 a = str(hasil0.loc[i, 'PROD_GROUP_NIEL'])
 b = str(hasil0.loc[i, 'PROD_GROUP_GEN'])
 ratio =  getSimularityPartialScore(a,b)
 if ratio >= 65:
   ratio1 = hasil0.loc[i]
   hasil0akur = hasil0akur.append(ratio1, ignore_index=True)
 else :
  continue
# hasil0akur

"""###data siang interval 6000"""

hasil1 = pd.DataFrame()
for i in range(len(dataSCM["PROGRAM_NAME_NIEL"])):
  #if len(data1.loc[i,"TX_TIME"]) >
 if dataSCM.loc[i,"AIRING_TIME_GEN"] <= '04300000':
  conv_gen = int(dataSCM.loc[i,'AIRING_TIME_GEN']) + 24000000
  hasil = int(dataSCM.loc[i,'AIRING_TIME_NIEL']) - conv_gen
 else : 
  hasil = int(dataSCM.loc[i,'AIRING_TIME_NIEL']) - int(dataSCM.loc[i,'AIRING_TIME_GEN']) 
 if hasil <= 6000 :
    if hasil >= -6000 :
     sel2 = dataSCM.loc[i]
     hasil1 = hasil1.append(sel2, ignore_index=True)
 else :
    continue
# hasil1

"""###FUZZY WUZZY interval 6000
###prod_name dan product_name
"""

hasil1akur  = pd.DataFrame()
for i in range(len(hasil1['PRODUCT_NAME_GEN'])):
 a = str(hasil1.loc[i, 'PRODUCT_NAME_GEN'])
 b = str(hasil1.loc[i, 'PRODUCT_NAME_NIEL'])
 ratio =  getSimularityPartialScore(a,b)
 if ratio >= 65 :
   ratio1 = hasil1.loc[i]
   hasil1akur = hasil1akur.append(ratio1, ignore_index=True)
 else :
  continue
# hasil1akur

"""###prod_name dan prod_GROUP"""

def getSimularityPartialScore(str1,str2):
    return fuzz.token_set_ratio(str1.lower(), str2.lower())


for i in range(len(hasil1['PRODUCT_NAME_GEN'])):
 a = str(hasil1.loc[i, 'PRODUCT_NAME_GEN'])
 b = str(hasil1.loc[i, 'PROD_GROUP_NIEL'])
 ratio =  getSimularityPartialScore(a,b)
 if ratio >= 65 :
   ratio1 = hasil1.loc[i]
   hasil1akur = hasil1akur.append(ratio1, ignore_index=True)
 else :
  continue

for i in range(len(hasil1['PRODUCT_NAME_NIEL'])):
 a = str(hasil1.loc[i, 'PRODUCT_NAME_NIEL'])
 b = str(hasil1.loc[i, 'PROD_GROUP_GEN'])
 ratio =  getSimularityPartialScore(a,b)
 if ratio >= 65 :
   ratio1 = hasil1.loc[i]
   hasil1akur = hasil1akur.append(ratio1, ignore_index=True)
 else :
  continue

for i in range(len(hasil1['PROD_GROUP_NIEL'])):
 a = str(hasil1.loc[i, 'PROD_GROUP_NIEL'])
 b = str(hasil1.loc[i, 'PROD_GROUP_GEN'])
 ratio =  getSimularityPartialScore(a,b)
 if ratio >= 65 :
   ratio1 = hasil1.loc[i]
   hasil1akur = hasil1akur.append(ratio1, ignore_index=True)
 else :
  continue

"""###GABUNGIN intv 900 dan 6000"""

hasil_gb_siang = pd.DataFrame()
hasil0akur  = hasil0akur.drop_duplicates()
hasil1akur  = hasil1akur.drop_duplicates()
hasil_gb_siang = hasil0akur.append(hasil1akur, ignore_index=True)
print("=========================================")
print("Shape :", hasil_gb_siang.shape)
print("DATA INTERVAL SUCCESS CREATED")

"""###SORT AIRING_TIME"""
df_siang_air = pd.DataFrame()
hasil_gb_siang = hasil_gb_siang.sort_values(by=['AIRING_TIME_NIEL'])
df_siang_air = df_siang_air.append(hasil_gb_siang, ignore_index=True)

for i in range(1,len(df_siang_air['AIRING_TIME_NIEL'])):
 if df_siang_air.loc[i,'AIRING_TIME_NIEL'] == df_siang_air.loc[i-1, 'AIRING_TIME_NIEL']:
    df_siang_air.drop([i-1], axis=0, inplace=True)

"""###SORT TX_TIME"""
df_siang_tx = pd.DataFrame()
df_siang_air = df_siang_air.sort_values(by=['AIRING_TIME_GEN'])
df_siang_tx = df_siang_tx.append(df_siang_air, ignore_index=True)

for i in range(1,len(df_siang_tx['AIRING_TIME_GEN'])):
 if df_siang_tx.loc[i,'AIRING_TIME_GEN'] == df_siang_tx.loc[i-1, 'AIRING_TIME_GEN']:
    df_siang_tx.drop([i-1], axis=0, inplace=True)

"""##GABUNGIN yang malem dan siang"""

hasil_akhir = pd.DataFrame()
hasil_akhir = df_siang_tx
# hasil_akhir.shape

"""#FINAL"""

hasil_tmp = pd.DataFrame()
hasil_akhir = hasil_akhir.sort_values(by=['AIRING_TIME_NIEL'])
hasil_tmp = hasil_tmp.append(hasil_akhir, ignore_index=True)
print("=========================================")
print("Shape :", hasil_tmp.shape)
print("HASIL MATCH SUCCESS CREATED")
#print(hasil_tmp)


#hasil_tmp.to_excel('match_sctv.xlsx')
with open("log_sch2.txt", "a") as f:
      now = datetime.now()
      date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
      print("====================================", file = f)
      print("Date and Time:",date_time, file=f)
      end = time.time()
      print("Duration Running (Sec) : ",end - start, file = f)
      print("Shape gen awal :", gn1.shape, file=f)
      print("Shape niel awal :", ni1.shape, file=f)
      print("Shape hasil : ", hasil_tmp.shape, file=f)
      print(hasil_tmp.head(), file=f)

# create_table = """CREATE TABLE MATCHING_SCTV(AIRING_DATE_GEN DATE,
#                 AIRING_DATE_NIEL DATE,
#                 AIRING_TIME_GEN VARCHAR(25),
#                 AIRING_TIME_NIEL VARCHAR(25),
#                 PROGRAM_NAME_NIEL VARCHAR(100),
#                 PROGRAM_NAME_GEN VARCHAR(100),
#                 PRODUCT_NAME_GEN VARCHAR(100),
#                 PRODUCT_NAME_NIEL VARCHAR(100),
#                 PROD_GROUP_NIEL VARCHAR(100),
#                 PROD_GROUP_GEN VARCHAR(100),
#                 PROD_VERSION_NAME_GEN	VARCHAR(100),
#                 MO_NO VARCHAR(30),
#                 PO_TYPE VARCHAR(30),
#                 PROD_CODE VARCHAR(30),
#                 PROD_VERSION VARCHAR(30),
#                 ROW_ID_SLOT VARCHAR(50),
#                 ROW_ID_SPOT	VARCHAR(50),
#                 RID_NIELSEN	VARCHAR(50),
#                 CALC_DATE DATE)"""
con = cx_Oracle.connect('sctv_read','sctv_read',cx_Oracle.makedsn('172.17.32.62',1521,'GENSCTV'))
cur = con.cursor()
# cur.execute("DROP TABLE MATCHING_SCTV")
# cur.execute(create_table)

insert_data = '''INSERT INTO MATCHING_SCTV(AIRING_DATE_GEN,
                AIRING_DATE_NIEL,
                AIRING_TIME_GEN,
                AIRING_TIME_NIEL,
                PROGRAM_NAME_NIEL,
                PROGRAM_NAME_GEN,
                PRODUCT_NAME_GEN,
                PRODUCT_NAME_NIEL,
                PROD_GROUP_NIEL,
                PROD_GROUP_GEN,
                PROD_VERSION_NAME_GEN,
                MO_NO,
                PO_TYPE,
                PROD_CODE,
                PROD_VERSION,
                ROW_ID_SLOT,
                ROW_ID_SPOT,
                RID_NIELSEN,
                CALC_DATE) 
                VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19)'''
rows = [tuple(x) for x in hasil_tmp.values]
cur.executemany(insert_data, rows)
con.commit()
cur.close()
con.close()

end = time.time()
print("Duration (s) : ",end - start)
print("Table Match Succesfully Created")
