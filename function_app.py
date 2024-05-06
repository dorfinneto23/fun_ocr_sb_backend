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

# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

#ocument intelligence Details 
document_intelligence_endpoint = os.environ.get('document_intelligence_endpoint')
document_intelligence_key = os.environ.get('document_intelligence_key')

tempurl = "https://www.btl.gov.il/About/Documents/FileSizeReductionGuide.pdf"
# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'

# Generic Function to update case  in the 'cases' table
def update_case_generic(caseid,field,value):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute(f"UPDATE cases SET {field} = ? WHERE id = ?", (value, caseid))
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

def analyze_document_and_save_markdown(blob_sas_url,caseid,filename):
    
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
            AnalyzeDocumentRequest(url_source=tempurl),  # Correct usage
            output_content_format=ContentFormat.MARKDOWN,
        )
        result = poller.result()
        pdfContent = result.content
        logging.info(f"sanalyze_document_and_save_markdown pdfContent: {pdfContent}")
        #data=pdfContent.read()
        blob_client = container_client.upload_blob(name=destinationPath, data=pdfContent)
        logging.info(f"sanalyze_document_and_save_markdown ocr url: {blob_client.url}")
        #preparing data for response 
        data = { 
            "status" : "sucess", 
            "blob url" :blob_client.url,
            "filename" :filename
        } 
        json_data = json.dumps(data)
        logging.info(f"sanalyze_document_and_save_markdown json response : {json_data}")
        return json_data
    except Exception as e:
        #preparing data for response 
            data = { 
                "status" : "error", 
                "Description" :str(e)
            } 
            json_data = json.dumps(data)
            logging.info(f"sanalyze_document_and_save_markdown json response : {json_data}")
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
    path = message_data_dict['path']
    url = message_data_dict['url']
    doc_id = message_data_dict['docid']
    logging.info(f"Before analyze_document_and_save_markdown")
    ocr_result = analyze_document_and_save_markdown(url,caseid,filename)
    ocr_result_dic = json.loads(ocr_result)
    if ocr_result_dic["status"] == "success":
        logging.info(f"ocr completed")
    else:
        errorMesg = ocr_result_dic["Description"]
        logging.info(f"error:{errorMesg}")

    

