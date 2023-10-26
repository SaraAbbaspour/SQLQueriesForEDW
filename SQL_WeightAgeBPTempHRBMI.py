import pandas as pd
import pyodbc
import math

# In[]: Load patient encounterIDs
EncounterID = pd.read_csv('')

# In[]: extracting data from EWD using encounterIDs
def ExtractDataFromEDW (EncounterID):
    N = 10000
    iterates = math.ceil(len(EncounterID)/N)
    PatientAdmitWeight = []
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
            t2.PatientIdentityID AS MRN
            	,t1.[PatientID]
            ,t1.[PatientEncounterID]
            	,t1.[DepartmentID]
            	,t1.[DepartmentDSC]
            	,t1.[PatientAgeNBR]
            ,t1.[BloodPressureSystolicNBR]
            ,t1.[BloodPressureDiastolicNBR]
            ,t1.[TemperatureFahrenheitNBR]
            ,t1.[HeartRateNBR]
            ,t1.[WeightOunceNBR]
            ,t1.[HeightTXT]
            ,t1.[BodyMassIndexNBR]
            	,t1.[HospitalAdmitDTS]
            	,t1.[EntryTimeDTS]
         FROM
             [Epic].[Encounter].[PatientEncounter_Enterprise] t1
             LEFT JOIN Epic.Patient.Identity_Enterprise t2 ON t2.PatientID=t1.PatientID
         WHERE
         	t2.IdentityTypeID=67
         	AND t1.WeightOunceNBR is not null
         	AND t1.PatientEncounterID in {EncounterIDs}
         ORDER BY t1.PatientEncounterID
          """
        PatientAdmitWeight_ = pd.read_sql_query(sql, con=conn)
        PatientAdmitWeight.append(PatientAdmitWeight_)
    return PatientAdmitWeight

PatientAdmitWeight = ExtractDataFromEDW(EncounterID)   
PatientAdmitWeight = pd.concat(PatientAdmitWeight)
PatientAdmitWeight = PatientAdmitWeight.reset_index(drop=True)

# In[3]: save data
# PatientAdmitWeight.to_csv (r'C:/Users/SaraA/Documents/Data/CV-Study/PatientAdmitWeightBMITempBP.csv', index = False, header=True)




