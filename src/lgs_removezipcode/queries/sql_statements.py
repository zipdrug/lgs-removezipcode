CHECK_LEADS_PATIENT_ID = """
SELECT
	DISTINCT leads.patient_id AS patientid,
	leads.plan_type AS lob,
	'reassign' as operationtype
FROM
	leads INNER JOIN patients ON
	patients.id = leads.patient_id
	AND patients.deleted_at IS NULL
INNER JOIN addresses ON
	addresses.patient_id = patients.id
	AND addresses.order = 0
	AND addresses.deleted_at IS NULL
WHERE
	leads.plan_type = 'MA'
	AND leads.potential_pharmacy_id = '{id}'
	AND addresses.postal_code = '{postalcode}'
"""
