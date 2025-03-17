import os
import azure.functions as func
import logging
import requests
import zipfile
from bs4 import BeautifulSoup
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

#Obtener cliente de Azure Data Lake
def get_service_client_token_credential(self, account_name) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    token_credential = DefaultAzureCredential()

    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    url = 'https://rebrickable.com/downloads/'

    try:
        datalake_client = get_service_client_token_credential(os.getenv('STORAGE_ACCOUNT_NAME'))

        #Inicio Extraer enlaces de la página
        # Obtener el HTML de la página
        res = requests.get(url)
        res.raise_for_status()  # Lanza un error si la respuesta no es 200

        # Parsear la página
        soup = BeautifulSoup(res.text, 'html.parser')
        
        #Recorrer página hasta lista de enlaces
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
            a = span.find('a') if span else None
            if a and 'href' in a.attrs:
                links.append(a['href'])
                print(a['href'])
       
        #Fin Extraer enlaces de la página

        #Descargar los archivos
        for link in links:
            response = requests.get(link)
            # Abrir el archivo ZIP desde la memoria
            zip_bytes = io.BytesIO(response.content)  # Cargar en memoria
            with zipfile.ZipFile(zip_bytes, 'r') as z:
                print(z)

                
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
