import pandas as pd
import json
from queries.sql_statements import CHECK_LEADS_PATIENT_ID

def get_pharmacy_details(queue_info: dict):
    pharmacy_info = {}
    msg_content = queue_info.get("Messages")
    #print("Ext Messages ",msg_content)
    #if i.get("Body")
    for i in msg_content:
        pharmacy_info = i.get("Body")
    pharmacy_json = json.loads(pharmacy_info)
    #print('patient_json', pharmacy_json)
    pharmacy_id = pharmacy_json.get("pharmacy_id")
    zipcode = pharmacy_json.get("zipcode")
    return pharmacy_id, zipcode

def get_patient_detail(engine, pharmacy_df: dict):
    final_patient_df = pd.DataFrame()
    for ind in pharmacy_df.index:
        pharmacy_id = pharmacy_df['pharmacyid'][ind]
        zipcode = pharmacy_df['zipcode'][ind]
        leads_df = leads_patient_id_extract(engine, pharmacy_id, zipcode)
        final_patient_df = pd.concat([final_patient_df, leads_df], ignore_index=True)

    #print("final_patient_df ", final_patient_df)
    return final_patient_df

def leads_patient_id_extract(engine,pharmacy_id, zipcode):
    ma_lead_patient_id_query = CHECK_LEADS_PATIENT_ID.format(id=pharmacy_id, postalcode=zipcode)
    patient_id_df = pd.read_sql(sql=ma_lead_patient_id_query, con=engine)

    #print("patient_id_df", patient_id_df)
    return patient_id_df
