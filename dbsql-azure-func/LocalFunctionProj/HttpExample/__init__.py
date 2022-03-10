import logging
import os
import azure.functions as func
from databricks import sql

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger to take a parameter and call a Databricks SQL query.')

    _limit = req.params.get('limit')
    if not _limit:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            _limit = req_body.get('limit')

    connection = sql.connect(
    server_hostname=os.environ['DATABRICKS_HOSTNAME'],
    http_path=os.environ['DATABRICKS_HTTP_PATH'],
    access_token=os.environ['DATABRICKS_ACCESS_TOKEN'])

    cursor = connection.cursor()

    cursor.execute('select num from (SELECT id as num FROM RANGE(10) ) nums where num < %(num_limit)s ', { 'num_limit' : _limit})
    result = cursor.fetchall()
    resp = 0
    for row in result:
        resp = resp + row[0]

    cursor.close()
    connection.close()


    if _limit:
        return func.HttpResponse(f"The sum of 1 to {_limit} is {resp}.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a limit in the query string or in the request body for a personalized response.",
             status_code=200
        )
