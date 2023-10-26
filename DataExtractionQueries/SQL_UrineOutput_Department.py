import pandas as pd
import pyodbc
import math

# In[]: Load patient encounterIDs
EncounterID = pd.read_csv('')

# In[]: extracting data from EWD using encounterIDs
def ExtractDataFromEDW (EncounterID):
    N = 10000
    iterates = math.ceil(len(EncounterID)/N)
    UrineOutput_Department = []
    for i in range(iterates):
        ### SERVER ACCESS
        conn_str = ('DRIVER={SQL Server};'
                    'SERVER=;'
                    'database=Epic;')
        conn = pyodbc.connect(conn_str)
        EncounterIDs = []
        if (i+1)*N < len(EncounterID):
            print(i)
            EncounterIDs = tuple(EncounterID[i*N:(i+1)*N]['EncounterID'])
        else:
            print('final   ', i)
            EncounterIDs = tuple(EncounterID[i*N:]['EncounterID'])
        sql = f"""
        SELECT DISTINCT
            	 t5.PatientIdentityID as MRN
            	,t1.PatientEncounterID
            ,t1.DepartmentDSC
            	,t1.DepartmentID
            	,t9.FlowsheetMeasureNM
            ,t9.DisplayNM
            	,t9.OutputTypDSC
            	,t8.RecordedDTS 
            ,t8.EntryTimeDTS
            ,t8.MeasureTXT
            ,t8.MeasureCommentTXT
            	,t9.UnitsCD
        FROM
            	Epic.Encounter.ADT_Enterprise t1
            	inner join Epic.Encounter.ADT_Enterprise t2 ON t1.LastInADTEventID=t2.EventID
            	inner join Epic.Patient.Identity_Enterprise t5 ON t1.PatientID=t5.PatientID
            	inner join Epic.Encounter.PatientEncounter_Enterprise t6 ON t6.PatientEncounterID=t1.PatientEncounterID
            	inner JOIN Epic.Clinical.FlowsheetRecordLink_Enterprise t7 ON t7.PatientID=t2.PatientID
            	inner JOIN Epic.Clinical.FlowsheetMeasure_Enterprise t8 ON t8.FlowsheetDataID = t7.FlowsheetDataID
            inner JOIN Epic.Clinical.FlowsheetGroup_Enterprise t9 ON t8.FlowsheetMeasureID = t9.FlowsheetMeasureID
        WHERE
            	(t6.HospitalAdmitDTS BETWEEN '2016-02-01 00:00' AND '2021-02-01 00:00')
            	and t5.IdentityTypeID=67
            	and t9.FlowsheetMeasureID in ('304550','61')
            	and t8.RecordedDTS BETWEEN t2.EffectiveDTS AND t1.EffectiveDTS
            	and t1.PatientEncounterID in {EncounterIDs}
     	order by t1.PatientEncounterID
          """
        UrineOutput_Department_ = pd.read_sql_query(sql, con=conn)
        UrineOutput_Department.append(UrineOutput_Department_)
    return UrineOutput_Department

UrineOutput_Department = ExtractDataFromEDW(EncounterID)   
UrineOutput_Department = pd.concat(UrineOutput_Department)
UrineOutput_Department = UrineOutput_Department.reset_index(drop=True)  

# In[]: save data
# UrineOutput_Department.to_csv (r'C:/Users/SaraA/Documents/Data/CV-Study/UrineOutputDepartment2.csv', index = False, header=True)




