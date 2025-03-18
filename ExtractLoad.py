import io
from bs4 import BeautifulSoup
import requests
import datetime
import gzip
''' 

# Función para obtener el cliente de Azure Data Lake
def get_service_client_token_credential(account_name) -> DataLakeServiceClient:
    # Construye la URL de tu cuenta de Data Lake
    account_url = f"https://{account_name}.dfs.core.windows.net"
    
    # Usar DefaultAzureCredential para obtener un token de autenticación
    token_credential = DefaultAzureCredential()

    # Crear el cliente de servicio de Data Lake
    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client

# Usar la función para obtener el cliente de Data Lake
datalake_client = get_service_client_token_credential(os.getenv('STORAGE_ACCOUNT_NAME'))
print(datalake_client)



'''
url = 'https://rebrickable.com/downloads/'
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
files = []


for i in li:
    span = i.find('span')
    a = span.find('a')
    links.append(a['href'])    
    
for link in links:
    filename = link.split('/')[-1].split('.gz')[0]
    response = requests.get(link)
    # Abrir el archivo ZIP desde la memoria
    gz_file_bytes = io.BytesIO(response.content) 
    # Cargar en memoria
    with gzip.GzipFile(fileobj=gz_file_bytes, mode='rb') as gz:
        file_content = gz.read()
        file = {filename, file_content}
        files.append(file_content)

'''
    if file_name.endswith('.gz'):
    with zip_ref.open(file_name) as gz_file:
    with gzip.open(gz_file, 'rb') as f_in:
    file_content = f_in.read()
'''
           
# Aquí puedes guardar el contenido del archivo descomprimido
    
