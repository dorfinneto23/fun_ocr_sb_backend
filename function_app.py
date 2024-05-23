import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
from PyPDF2 import PdfReader,PdfWriter  # in order to read and write  pdf file 
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import ContentFormat, AnalyzeDocumentRequest
from azure.core.credentials import AzureKeyCredential
from azure.data.tables import TableServiceClient, TableClient, UpdateMode # in order to use azure storage table  
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError # in order to use azure storage table  exceptions 

# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

#ocument intelligence Details 
document_intelligence_endpoint = os.environ.get('document_intelligence_endpoint')
document_intelligence_key = os.environ.get('document_intelligence_key')

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'



#  Function check how many rows in partition of azure storage table
def count_rows_in_partition( table_name,partition_key):
    # Create a TableServiceClient object using the connection string
    service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)
    
    # Get the table client
    table_client = service_client.get_table_client(table_name=table_name)
    
    # Define the filter query to count entities with the specified partition key and where ocrUrl is not null or empty
    filter_query = f"PartitionKey eq '{partition_key}' and ocrUrl ne ''"
    
    # Query the entities and count the number of entities
    entities = table_client.query_entities(query_filter=filter_query)
    count = sum(1 for _ in entities)  # Sum up the entities
    
    if count>0:
        return count
    else:
        return 0


# Update field on specific entity/ row in storage table 
def update_documents_entity_field(table_name, partition_key, row_key, field_name, new_value,field_name2,new_value2,field_name3,new_value3):
    """
    Updates a specific field of an entity in an Azure Storage Table.

    Parameters:
    - account_name: str, the name of the Azure Storage account
    - account_key: str, the key for the Azure Storage account
    - table_name: str, the name of the table
    - partition_key: str, the PartitionKey of the entity
    - row_key: str, the RowKey of the entity
    - field_name: str, the name of the field to update
    - new_value: the new value to set for the field
    """
    try:
        # Create a TableServiceClient using the connection string
        table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient
        table_client = table_service_client.get_table_client(table_name)

        # Retrieve the entity
        entity = table_client.get_entity(partition_key, row_key)

        # Update the field
        entity[field_name] = new_value
        entity[field_name2] = new_value2
        entity[field_name3] = new_value3

        # Update the entity in the table
        table_client.update_entity(entity, mode=UpdateMode.REPLACE)
        logging.info(f"update_documents_entity_field:Entity updated successfully.")

    except ResourceNotFoundError:
        logging.info(f"The entity with PartitionKey '{partition_key}' and RowKey '{row_key}' was not found.")
    except Exception as e:
        logging.info(f"An error occurred: {e}")

# Generic Function to update case  in the 'cases' table
def update_case_generic(caseid,field,value,field2,value2):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute(f"UPDATE cases SET {field} = ?, {field2} = ? WHERE id = ?", (value, value2, caseid))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info(f"case {caseid} updated field name: {field} , value: {value}")
        return True
    except Exception as e:
        logging.error(f"Error update case: {str(e)}")
        return False    
    
    #Create event on azure service bus 
def create_servicebus_event(queue_name, event_data):
    try:
        # Create a ServiceBusClient using the connection string
        servicebus_client = ServiceBusClient.from_connection_string(connection_string_servicebus)

        # Create a sender for the queue
        sender = servicebus_client.get_queue_sender(queue_name)

        with sender:
            # Create a ServiceBusMessage object with the event data
            message = ServiceBusMessage(event_data)

            # Send the message to the queue
            sender.send_messages(message)

        print("Event created successfully.")
    
    except Exception as e:
        print("An error occurred:", str(e))

def analyze_document_and_save_markdown(fileUrl,caseid,filename):
    
    try:
        logging.info(f"sanalyze_document_and_save_markdown: Start")
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        filename = filename.replace('.pdf', '.txt')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        destinationPath = f"{basicPath}/ocr/{filename}"
        logging.info(f"sanalyze_document_and_save_markdown Destination Path: {destinationPath}")

        document_intelligence_client = DocumentIntelligenceClient(
            endpoint=document_intelligence_endpoint, 
            credential=AzureKeyCredential(document_intelligence_key)
        )
        logging.info(f"document_intelligence_client -  defined")

        poller = document_intelligence_client.begin_analyze_document(
            "prebuilt-layout",
            AnalyzeDocumentRequest(url_source=fileUrl),  # Correct usage
            output_content_format=ContentFormat.MARKDOWN,
        )
        result = poller.result()
        pdfContent = result.content
        logging.info(f"analyze_document_and_save_markdown pdfContent: {pdfContent}")
        #data=pdfContent.read()
        blob_client = container_client.upload_blob(name=destinationPath, data=pdfContent)
        logging.info(f"sanalyze_document_and_save_markdown ocr url: {blob_client.url}")
        #preparing data for response 
        data = { 
            "status" : "success", 
            "bloburl" :blob_client.url,
            "filename" :filename,
            "path" :destinationPath,
            "Description" : f"OCR Process file:{filename} sucess"
        } 
        json_data = json.dumps(data)
        logging.info(f"analyze_document_and_save_markdown sucess response : {json_data}")
        return json_data
    except Exception as e:
        #preparing data for response 
            data = { 
                "status" : "error", 
                "Description" :str(e)
            } 
            json_data = json.dumps(data)
            logging.info(f"analyze_document_and_save_markdown error response : {json_data}")
            return json_data

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="ocr",
                               connection="medicalanalysis_SERVICEBUS") 
def sb_ocr_process(azservicebus: func.ServiceBusMessage):
    message_data = azservicebus.get_body().decode('utf-8')
    logging.info('Received messageesds: %s', message_data)
    message_data_dict = json.loads(message_data)
    caseid = message_data_dict['caseid']
    filename = message_data_dict['filename']
    filename_without_extension = os.path.splitext(filename)[0]
    new_filename = filename_without_extension + ".text"
    path = message_data_dict['path']
    url = message_data_dict['url']
    doc_id = message_data_dict['docid']
    pagenumber = message_data_dict['pagenumber'] #the page number on pdf files 
    totalpages = message_data_dict['pages_num'] #total pages need to ocr operation 
    logging.info(f"Before analyze_document_and_save_markdown")
    ocr_result = analyze_document_and_save_markdown(url,caseid,filename)
    ocr_result_dic = json.loads(ocr_result)
    if ocr_result_dic["status"] == "success":
        logging.info(f"ocr completed")
        #preparing data for service bus 
        data = { 
                "doc_id" : doc_id, 
                "filename" :new_filename,
                "path" :ocr_result_dic["path"],
                "url" :ocr_result_dic["bloburl"],
                "caseid" :caseid,
                "pagenumber" :pagenumber,
                "totalpages" :totalpages
            } 
        json_data = json.dumps(data)
        create_servicebus_event("contentanalysis", json_data)
        update_documents_entity_field("documents",caseid,doc_id,"status",2,"ocrPath",ocr_result_dic["path"],"ocrUrl",ocr_result_dic["bloburl"]) #update status to 2 "ocr done"
        logging.info(f"the ocr page number is {pagenumber} out of {totalpages}")
        pages_done = count_rows_in_partition("documents",caseid) # check how many pages proccess done 
        if totalpages==pages_done: #check if the last file passed 
            update_case_generic(caseid,"status",5,"ocrProcess",1) #update case status to 6 "ocr done" and ocrProcess = 1 done
    else:
        errorMesg = ocr_result_dic["Description"]
        logging.info(f"error:{errorMesg}")
        update_documents_entity_field("documents",caseid,doc_id,"status",3) #update document status to 2 "ocr failed"
        update_case_generic(caseid,"status",6,"ocrProcess",2) #update case status to 6 "ocr failed" and ocrProcess 2 failed

    

