import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
from PyPDF2 import PdfReader,PdfWriter  # in order to read and write  pdf file 
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="ocr",
                               connection="medicalanalysis_SERVICEBUS") 
def sb_ocr_process(azservicebus: func.ServiceBusMessage):
    logging.info('Python ServiceBus Queues trigger processed a message: %s',
                azservicebus.get_body().decode('utf-8'))
