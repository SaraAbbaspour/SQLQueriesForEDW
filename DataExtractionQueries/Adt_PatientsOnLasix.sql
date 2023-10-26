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
			(t1.MedicationDSC LIKE '%LASIX%' OR t1.MedicationDSC LIKE '%furosemide%')
			and t1.OrderStatusCD  in (5,2,9,10)
			and t3.MedicationTakenDTS IS NOT NULL
			and t1.PatientLocationDSC LIKE '%MGH%'
			and t2.IdentityTypeID=67
			and t2.PatientIdentityID in ('1396464')
	)
	order by t6.HospitalAdmitDTS 