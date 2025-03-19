import json
import os
import io
import gzip
import azure.functions as func
import logging
import requests
import datetime
from bs4 import BeautifulSoup
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

today = datetime.date.today()
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

#Obtener cliente de Azure Data Lake
def get_service_client_token_credential(account_name) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client


#Obtener enlaces 
def getFileLinks(url):

    try:
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        body = soup.find('body')
        wrapper = body.find('div', {'id': 'wrapper'})
        content = wrapper.find('div', {'id': 'content'})
        container = content.find('div', {'class': 'container'})
        row = container.find('div', {'class': 'row'})
        col = row.find('div', {'class': 'col-md-5'})
        ul = col.find('ul', {'class': 'list-unstyled'})
        li = ul.find_all('li')

        links = [] 
    
        for i in li:
            span = i.find('span')
            a = span.find('a')
            links.append(a['href'])

    except Exception as e:
        logging.error(f"Error al obtener enlaces: {str(e)}")
    
    return links

#Descargar archivos
def downloadFiles(links):
    files = []
    for link in links:
        try:
            filename = link.split('/')[-1].split('.gz')[0]
            response = requests.get(link)
            # Abrir el archivo ZIP desde la memoria
            gz_file_bytes = io.BytesIO(response.content) 
            # Cargar en memoria
            with gzip.GzipFile(fileobj=gz_file_bytes, mode='rb') as gz:
                file_content = gz.read()
                file = {'filename':filename, 'filecontent':file_content}
                files.append(file)
        except requests.RequestException as e:
            logging.error(f'Error al descargar {link} error:{str(e)}')
        except gzip.BadGzipFile as e:
            logging.error(f'Error al extraer el fichero {link} error: {str(e)}')
    
    return files

#Subir Archivos al Data Lake
def uploadFiles(files, datalake_client):
    file_system_client = datalake_client.get_file_system_client('raw')
    file_system_directory = file_system_client.get_directory_client(directory='Rebrickable')
    for file in files:
        try:
            if not file_system_directory.get_file_client(file['filename']).exists():
                file_system_directory.create_directory(file['filename'])
            
            elif file_system_directory.get_file_client(file['filename']).exists():
                #Creo archivo vacío en dl y luego subo el contenido 
                dl_empty_file = file_system_directory.create_file(str(today))
                dl_file = dl_empty_file.append_data(data=file['filecontent'], offset=0, length=len(file['filecontent']))
                dl_file.flush_data(len(file['filecontent']))
                logging.info(f"Archivo {file['filename']} subido correctamente")
            else:
                logging.error(f"Error al subir archivo {file['filename']}")
        except Exception as e:
            logging.error(f'Error al subir el fichero {str(today)} al directorio {file['filename']} error: {str(e)}')

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    url = 'https://rebrickable.com/downloads/'

    try:
        datalake_client = get_service_client_token_credential(os.getenv('STORAGE_ACCOUNT_NAME'))
        links = getFileLinks(url)
        #Descargar los archivos
        files = downloadFiles(links)
        #Subir los archivos al Data Lake
        uploadFiles(files,datalake_client)

        # Respuesta en JSON
        return func.HttpResponse(
            json.dumps({"links": links}),
            mimetype="application/json",
            status_code=200
        )

    except requests.RequestException as e:
        logging.error(f"Error al hacer la solicitud: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": "Error al obtener la página", "details": str(e)}),
            mimetype="application/json",
            status_code=500
        )

    except Exception as e:
        logging.error(f"Error inesperado: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": "Error interno", "details": str(e)}),
            mimetype="application/json",
            status_code=500
        )
