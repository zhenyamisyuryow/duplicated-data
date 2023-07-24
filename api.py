from google.cloud import bigquery
import re
from google.oauth2 import service_account
import json
from flask import Flask, request, Request


def parseCSV(incoming_csv):
    return(tuple(re.findall('.*,(\S+@\S+),', incoming_csv)))

def clientQuery(parsedCSV):
    credentials = service_account.Credentials.from_service_account_file('credentials.json',scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    client = bigquery.Client(credentials=credentials,project=credentials.project_id,)
    query = """
        SELECT corporate_email as email
        FROM `okta-260114.research_db.leads_research_db`
        WHERE corporate_email IN {}
    """.format(parsedCSV)
    query_job = client.query(query)  # Make an API request. 
    return(query_job.result())

def proceedData(query_results):
    output = {
        'total' :  query_results.total_rows
    }
    try:
        duplicates = list()
        if query_results.total_rows > 0:
            for row in query_results:
                duplicates.append(row.get('email'))
            output.update({'emails' : duplicates})
            
    except:
        print('Error')
        output.update({'error' : True})
        
    return(json.dumps(output))

app = Flask(__name__)

@app.route('/', methods = ['POST'])
def submit():
    if request.method == 'POST':
        if 'csv' in request.files:
            incoming_csv = request.files['csv'].read()
            if incoming_csv:
                try:
                    incoming_csv = incoming_csv.decode('utf-8')
                    parsed_csv = parseCSV(incoming_csv)
                    query_result = clientQuery(parsed_csv)
                    final_result = proceedData(query_result)

                    response = app.response_class(
                        response = final_result,
                        status = 200,
                        mimetype = 'application/json'
                    )
                except:
                    print("Exception thrown")
                    response = app.response_class(
                        response = "unprocessable file",
                        status = 422,
                        mimetype = 'application/json'
                    )
            else :
                response = app.response_class(
                    response = "error",
                    status = 500,
                    mimetype = 'application/json'
                )
        else:
            response = app.response_class(
                response = "error",
                status = 422,
                mimetype = 'application/json'
            )
        return(response)

if __name__ == '__main__':
    app.debug = True
    app.run()