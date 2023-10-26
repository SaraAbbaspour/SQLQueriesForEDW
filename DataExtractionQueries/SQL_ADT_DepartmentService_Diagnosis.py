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
    ADTDepartmentServiceMRNEncounter = []
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
            	,t4.DiagnosisNM
            	,t4.DiagnosisGroupDSC
            	,t3.ContactDTS
            ,t1.DepartmentDSC
            	,t1.DepartmentID
            	,t1.PatientServiceDSC
            	,t1.PatientServiceCD
            	,t1.PatientClassDSC
            	,t1.PatientClassCD
            	,t1.ADTEventTypeDSC
            	,t1.ADTEventTypeCD
            ,t2.EffectiveDTS as TransferInDTS
            ,t1.EffectiveDTS as TransferOutDTS
            ,t8.ICD9
            ,replace(t8.ICD9DSC,',',' -') as ICD9DSC
            ,t8.ICD10
            ,t8.ICD10DSC
        FROM
            	Epic.Encounter.ADT_Enterprise t1
            	inner join Epic.Encounter.ADT_Enterprise t2 ON t1.LastInADTEventID=t2.EventID
            	inner join Epic.Patient.Identity_Enterprise t5 ON t1.PatientID=t5.PatientID
            	inner join Epic.Encounter.PatientEncounter_Enterprise t6 ON t6.PatientEncounterID=t1.PatientEncounterID
            	inner join Epic.Encounter.PatientEncounterDiagnosis_Enterprise t3 ON t3.PatientEncounterID=t1.PatientEncounterID
            	inner join Epic.Reference.ICDDiagnosis t4 ON t4.DiagnosisID=t3.DiagnosisID
            left join Epic.Reference.DiagnosisCurrentICD9 t7 on t3.DiagnosisID=t7.DiagnosisID
            left join Misc.Reference.ICD9toICD10Mapping t8 on t7.ICD9CD=t8.ICD9DottedCD
        WHERE
        	(t6.HospitalAdmitDTS BETWEEN '2016-04-01 00:00' AND '2021-02-01 00:00')
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
        	order by t5.PatientIdentityID 
          """
        ADTDepartmentServiceMRNEncounter_ = pd.read_sql_query(sql, con=conn)
        ADTDepartmentServiceMRNEncounter.append(ADTDepartmentServiceMRNEncounter_)
    return ADTDepartmentServiceMRNEncounter

ADTDepartmentServiceMRNEncounter = ExtractDataFromEDW(MRN_Encounter)   
ADTDepartmentServiceMRNEncounter_ = pd.concat(ADTDepartmentServiceMRNEncounter)
ADTDepartmentServiceMRNEncounter_ = ADTDepartmentServiceMRNEncounter_.reset_index(drop=True)

# In[]: save data
# ADTDepartmentServiceMRNEncounter_.to_csv (r'C:/Users/SaraA/Documents/Data/CV-Study/ADTDiagnosisNM.csv', index = False, header=True)




