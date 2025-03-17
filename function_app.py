import azure.functions as func
import logging
import requests
from bs4 import BeautifulSoup
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    url = 'https://rebrickable.com/downloads/'

    try:
        # Obtener el HTML de la página
        res = requests.get(url)
        res.raise_for_status()  # Lanza un error si la respuesta no es 200

        # Parsear la página
        soup = BeautifulSoup(res.text, 'html.parser')
        body = soup.find('body')
        wrapper = body.find('div', {'id': 'wrapper'})
        content = wrapper.find('div', {'id': 'content'})
        container = content.find('div', {'class': 'container'})
        row = container.find('div', {'class': 'row'})
        col = row.find('div', {'class': 'col-md-5'})
        ul = col.find('ul', {'class': 'list-unstyled'})
        li = ul.find_all('li')

        # Extraer los enlaces
        links = []
        for i in li:
            span = i.find('span')
            a = span.find('a') if span else None
            if a and 'href' in a.attrs:
                links.append(a['href'])
                print(a['href'])


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
