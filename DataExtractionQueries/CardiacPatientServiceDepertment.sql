SELECT DISTINCT
    t1.DepartmentDSC
	,t1.PatientServiceDSC
FROM
	Epic.Encounter.ADT_Enterprise t1
	inner join Epic.Encounter.ADT_Enterprise t2 ON t1.LastInADTEventID=t2.EventID
	inner join Epic.Patient.Identity_Enterprise t5 ON t1.PatientID=t5.PatientID
	inner join Epic.Encounter.PatientEncounter_Enterprise t6 ON t6.PatientEncounterID=t1.PatientEncounterID
WHERE
	 t5.IdentityTypeID=67
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
			and t3.RouteDSC in ('Intravenous', 'intravenous push')
			and t1.PatientLocationDSC like '%MGH%'
			and t2.IdentityTypeID=67
	)
	and t1.DepartmentDSC in ('MGH ELLISON 10 STP DWN')
	order by t1.PatientServiceDSC ASC


