import pandas as pd
import pyodbc
import math

# In[]: Load patient MRN and encounterIDs
def readPrepareData (DataToBePrepared):
    DataToBePrepared.columns = DataToBePrepared.columns.str.replace(' ', '')
    DataToBePrepared = DataToBePrepared.drop([0])
    DataToBePrepared = DataToBePrepared.drop(DataToBePrepared.index[len(DataToBePrepared)-1])
    DataToBePrepared[['MRN', 'Encounter']] = DataToBePrepared['MRNPatientEncounterID'].str.split(' ', 1, expand=True)
    EncounterMRN_1 = DataToBePrepared.Encounter.str.replace(' ', '')
    EncounterMRN_2 = DataToBePrepared[['MRN']]
    EncounterMRN = pd.concat([EncounterMRN_2, EncounterMRN_1.to_frame()], axis=1)
    return EncounterMRN

MRN_Encounter = pd.read_csv('')
MRN_Encounter = readPrepareData(MRN_Encounter)

# In[]: preparing the EncounterIDs for extracting data from EWD
def ExtractDataFromEDW (MRN_Encounter):
    N = 10000
    iterates = math.ceil(len(MRN_Encounter)/N)
    AdmitDischargeDTS = []
    for i in range(iterates):
        ### SERVER ACCESS
        conn_str = ('DRIVER={SQL Server};'
                    'SERVER=;'
                    'database=Epic;')
        conn = pyodbc.connect(conn_str)
        EncounterIDs = []
        if (i+1)*N < len(MRN_Encounter):
            print(i)
            EncounterIDs = tuple(MRN_Encounter[i*N:(i+1)*N]['Encounter'])
        else:
            print('final   ', i)
            EncounterIDs = tuple(MRN_Encounter[i*N:]['Encounter'])
        sql = f"""
        SELECT DISTINCT
            	 t5.PatientIdentityID as MRN
            	,t1.PatientEncounterID
            	,t6.HospitalAdmitDTS
            	,t6.HospitalDischargeDTS
        FROM
            	Epic.Encounter.ADT_Enterprise t1
            	inner join Epic.Encounter.ADT_Enterprise t2 ON t1.LastInADTEventID=t2.EventID
            	inner join Epic.Patient.Identity_Enterprise t5 ON t1.PatientID=t5.PatientID
            	inner join Epic.Encounter.PatientEncounter_Enterprise t6 ON t6.PatientEncounterID=t1.PatientEncounterID
        WHERE
            	(t6.HospitalAdmitDTS BETWEEN '2016-02-01 00:00' AND '2021-02-01 00:00')
            	and t5.IdentityTypeID=67
            	and t1.PatientEncounterID in (
            		SELECT
            			t1.PatientEncounterID
            		FROM 
            			Epic.Orders.Medication_Enterprise t1   
            			inner join Epic.Patient.Identity_Enterprise t2 ON t1.PatientID=t2.PatientID
            			inner join Epic.Clinical.AdministeredMedication_Enterprise t3 ON t1.OrderID=t3.OrderID   
            		WHERE
            			(t1.MedicationDSC LIKE '%%LASIX%%' OR t1.MedicationDSC LIKE '%%furosemide%%')
            			and t1.OrderStatusCD  in (5,2,9,10)
            			and t3.MedicationTakenDTS IS NOT NULL
            			and t1.PatientLocationDSC LIKE '%%MGH%%'
            			and t2.IdentityTypeID=67
                        and t1.PatientEncounterID in {EncounterIDs}
            	)
        	order by t1.PatientEncounterID
          """
        AdmitDischargeDTS_ = pd.read_sql_query(sql, con=conn)
        AdmitDischargeDTS.append(AdmitDischargeDTS_)
    return AdmitDischargeDTS

AdmitDischargeDTS = ExtractDataFromEDW(MRN_Encounter)   
AdmitDischargeDTS_ = pd.concat(AdmitDischargeDTS)
AdmitDischargeDTS_ = AdmitDischargeDTS_.reset_index(drop=True)

# In[]:
AdmitDischargeDTS_ = AdmitDischargeDTS_.dropna(subset=['HospitalAdmitDTS'])
AdmitDischargeDTS_ = AdmitDischargeDTS_.dropna(subset=['HospitalDischargeDTS'])
AdmitDischargeDTS_ = AdmitDischargeDTS_.reset_index(drop=True)
 
# In[]: save data
# AdmitDischargeDTS_.to_csv (r'C:/Users/SaraA/Documents/Data/CV-Study/AdmitDischargeDTS.csv', index = False, header=True)




