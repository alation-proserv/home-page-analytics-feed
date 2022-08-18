# -------
# Author: Antonio Fernandez
# Alation Professional Services
# Last changed: 08/18/2022 01:09 PM 
#--------

import pandas as pd
import urllib3
urllib3.disable_warnings()
import requests
import sys
import configparser
import csv
#from bs4 import BeautifulSoup

# NOTE: this script assumes that the queries have been scheduled and ran once at least via the scheduler
# NOTE: this script assumes that each query result will not have more than 1000 rows 

########### CUSTOMER PARAMS #########
config = configparser.ConfigParser()
config.read("config_feed.ini")

HOST = config.get("api", "alation_base_url")
REFRESH_TOKEN = config.get("api", "refresh_token")
USER_ID = config.get("api", "user_id") # user id integer of user that will be creating the articles
CUSTOM_ARTICLE_TEMPLATE_ID = config.get("article", "template_id") # custom template id integer, can be found in the custom_templates table in Alation Analytics v2. This isn't required but the template name is
article_template = config.get("article", "article_template")

#Set variables with query ids
top_articles = 62
new_articles = 63
unanswered_questions = 64
new_queries = 68
# array of Alation Analytics queries
query_ids = [top_articles, new_articles, unanswered_questions, new_queries]

########## END CUSTOMER PARAMS ########

# Instance for all Alation API related calls
class AlationInstance():
    def __init__(self, host, headers):
        self.host = host
        self.headers = headers

    def generic_api_post(self, api, params=None, body=None):
        r = requests.post(self.host + api, json=body, params=params, headers=self.headers, verify=False)

        if r.status_code:
            # add try catch to this response. see line 50. try turning the response to json, if that fails for any reason, just return the content
            # then when handling the reponse outside of this class, just make sure that the response is valid json
            r_parsed = r.json()
            
            if 'job_id' in r_parsed:
                params = dict(id=r_parsed['job_id'])
                url_job = "/api/v1/bulk_metadata/job/"
                
                while (True):
                    status = self.generic_api_get(api=url_job, params=params)
                    if status['status'] != 'running':
                        objects = status['result']

                        break
                r_parsed = status
            return r_parsed
        else:
            return r.content

    def generic_api_get(self, api, headers=None, params=None):
        r = requests.get(self.host + api, headers=self.headers, params=params, verify=False)
        if r.status_code in [200, 201]:
            try:
                return r.json()
            except:
                return r.content 
        else:
            return r.content
    
    def generic_api_delete(self, api, headers=None, params=None):
        r = requests.delete(self.host + api, headers=self.headers, params=params, verify=False)
        return r

# Analytics Feed Formatting
def format_new_articles(query_results_list):

    new_articles = '<details>'
    new_articles += '<summary style="color: #666666;"><strong>Article new arrivals:</strong></summary>'
    new_articles += '<ul>'
    #new_articles += create_table_with_list(query_results_list, column_headers)
    i = 0;
    for na in query_results_list:
        if i != 0:
            new_articles += '<li> <a href = "' + str(na[1]) + '" >' + str(na[0]) + '</a> </li>'
        i = i+1

    new_articles += '</ul></details>'
    #print(new_articles)
    return new_articles

#function to format the list of top articles
def format_most_popular_articles(query_results_list):
    most_popular_articles = '<details>'
    most_popular_articles += '<summary style="color: #666666;"><strong>This week&#39;s most popular articles:</strong></summary>'
    most_popular_articles += '<ul>'
    i=0
    for pa in query_results_list:
        if i != 0:
            most_popular_articles += '<li><a href = "' + str(pa[1]) + '">' + str(pa[0])  +  '</a>' + ' by ' +str(pa[2]) +'</li>'
        i=i+1
    most_popular_articles += '</ul></details>'

    return most_popular_articles

#function to format the list of unanswered questions
def format_conversations(query_results_list):
    conversations = '<details>'
    conversations += '<summary style="color: #666666;"><strong>Questions that still need an answer:</strong></summary>'
    conversations += '<ul>'
    i=0
    for c in query_results_list:
        if i != 0:
            conversations += '<li><a href = "' + str(c[2]) + '">' + str(c[0])  +  '</a>' + ' asked by ' +str(c[1]) +'</li>'
        i=i+1
    conversations += '</ul></details>'

    return conversations

#function to format the list of new published queries
def format_new_queries(query_results_list):
    new_queries = '<details>'
    new_queries += '<summary style="color: #666666;"><strong>Newly published queries:</strong></summary>'
    new_queries += '<ul>'
    i=0
    for nq in query_results_list:
        if i != 0:
            new_queries += '<li><a href = "' + str(nq[3]) + '">' + str(nq[0])  +  '</a>' + ' by ' +str(nq[1]) +'</li>'
        i = i +1

    new_queries += '</ul></details>'

    return new_queries

# HTML transformations
def add_row_x_values_list(value_list):
    list_length = len(value_list)

    # Calculate the row width as a float with 4 decimals and then typecasting to string
    row_width = str(round(100 / list_length, 4))

    beginTag = "<tr>"
    endTag = "</tr>"

    row_value = ""

    # For the values of the Status column, background color is added to make the value really pop.
    for x in value_list:
        row_value = row_value + '<td style="width: ' + row_width + '%;">' + str(x) + '</td>'

    completedRowHTML = beginTag + row_value + endTag

    return str(completedRowHTML)

def create_table_with_list(value_list, column_headers):
    list_length = len(value_list)

    beginTableTag = '<table style="width: 100%; ">'
    beginBodyTag = '<tbody>'
    closeBodyTag = '</tbody>'
    closeTableTag = '</table>'

    row_value = add_row_x_values_list(column_headers)

    for x in value_list:
        row_value = row_value + add_row_x_values_list(x)

    finalTableHTML = beginTableTag + beginBodyTag + row_value + closeBodyTag + closeTableTag

    return str(finalTableHTML)

# first create API access token from refresh token
data = {
    "refresh_token": REFRESH_TOKEN,
    "user_id": USER_ID
}


#main logic
create_api_token_response = requests.post(f'{HOST}/integration/v1/createAPIAccessToken/', data=data)

api_access_token = None
try:
    api_access_token = create_api_token_response.json()['api_access_token']
except:
    print('could not get api access token')

if not api_access_token:
    print('No api access token available to make api calls')
    sys.exit()

headers = {'TOKEN': api_access_token, 'Content-Type': 'application/json'}

# login to Alation
alation = AlationInstance(HOST, headers)

# API endpoints and params
upload_logical_metadata_api = 'api/v1/bulk_metadata/custom_fields'
article_api = 'integration/v1/article'
custom_template_api = 'integration/v1/custom_template/'
object_type = 'article'
article_params = '?create_new=false&replace_values=true'

# testing purposes only
# article_template = 'Test Custom Template'

custom_template_id = None
if CUSTOM_ARTICLE_TEMPLATE_ID:
    custom_template_id = CUSTOM_ARTICLE_TEMPLATE_ID

# get custom template id if it has not been defined beforehand
if not custom_template_id:
    print('No custom template id found, retrieving custom template id')
    get_custom_templates_response = alation.generic_api_get(f'/{custom_template_api}/')
    
    if isinstance(get_custom_templates_response, list) and get_custom_templates_response:
        custom_template_titles = list(map(lambda x: x['title'], get_custom_templates_response))
        if article_template in custom_template_titles:
            custom_template_index = custom_template_titles.index(article_template)
            custom_template_id = get_custom_templates_response[custom_template_index]['id']
            print(f'Custom template was retrieved with id {custom_template_id}')
  
        else: 
            print(f'Article template: {article_template} could not be retrieved')
    else:
        print('Could not get all custom templates, please check the server and try again.')

if custom_template_id:
    custom_template_id = int(custom_template_id)
    custom_template_filter_params = f'?custom_field_templates=[{custom_template_id}]'
    custom_template_articles = alation.generic_api_get(f"/{article_api}/{custom_template_filter_params}")

    # adding html format
    html_table = '' #<h2 style="color: #FF6B33;">What&#39;s happening in A@A?</h2><br><br>'

    # iterate through query ids
    for query_id in query_ids:
        print('==============================')
        print(f'Getting query results for query id {query_id}')

        exec_session_params={
            'query_id': query_id
        }
        
        article_template_with_format = article_template.replace(' ', '%20')

        exec_session = alation.generic_api_get('/integration/v1/query/execution_session/', params=exec_session_params)

        # this assumes that the query has been ran once already
        if isinstance(exec_session, list) and exec_session:
            batch_id = exec_session[-1]['batch_ids'][0]
            batch = alation.generic_api_get(f'/integration/v1/query/execution_batch/{batch_id}/')

            if isinstance(batch, dict):
                event_id = batch['events'][0]['id']
                exec_event = alation.generic_api_get(f"/integration/v1/query/execution_event/{event_id}/")
                if isinstance(batch, dict) and exec_event['result']:
                    result_id = exec_event['result']['id']
                    data_schema = exec_event['result']['data_schema']
                    cols = [schema['name'] for schema in data_schema]
                    #API call to download the results from the last execution event
                    req = HOST + f'/integration/v1/result/{result_id}/csv/'
                    response = requests.get(req, headers=headers)
                    decoded_content = response.content.decode('utf-8')
                    csv_reader = csv.reader(decoded_content.splitlines(), delimiter=',')
                    query_results_list = list(csv_reader)
                    num_rows = len(query_results_list)
                    if isinstance(query_results_list, list) and query_results_list:
                        if query_id == top_articles: #most popular articles
                            html_table += format_most_popular_articles(query_results_list)

                        elif query_id == new_articles: #Article new arrivals
                            html_table += format_new_articles(query_results_list)

                        elif query_id == new_queries: #Newly published queries
                            html_table += format_new_queries(query_results_list)

                        elif query_id == unanswered_questions: #Questions that still need an answer
                            html_table += format_conversations(query_results_list)

                        html_table += '<br>'
                        #print(html_table)

                    else:
                        print(f'Could not get query results for query {query_id}, please make sure your server is running and try again or check the query and make sure it is published correctly')
                elif isinstance(batch, dict) and not exec_event['result']:
                    print(f'Could not get get exec event for query {query_id}, please republish and reschedule this query and try again')
                else:
                    print(f'Could not get get exec event for query {query_id}, please make sure your server is running and try again')
            else:
                print(f'Could not get get batch details for query {query_id}, please make sure your server is running and try again')
        else:
            print(f'Could not get any execution details for query {query_id}')
            print('Make sure the query has been executed already and that your Alation server is running')


    #print(html_table)
    # testing purposes only
data = {
    'key': 'What\'s happening in Alation?',
    'Recent Updates': html_table
    #'body': article_body
}

article_title = 'What\'s happening in Alation?'

article = alation.generic_api_get(f'/{article_api}?title={article_title}')
if isinstance(article, list) and article:
    article_id = article[0]['id']
    print(f'Article with title: "{article_title}" already exists and has id: {article_id}')

    # update the already existing article
    http_request = f'/{upload_logical_metadata_api}/{article_template_with_format}/{object_type}{article_params}'
    populate_custom_fields_response = alation.generic_api_post(http_request, body=data)
    if isinstance(populate_custom_fields_response, dict) and populate_custom_fields_response['updated_objects'] != 0 and \
            populate_custom_fields_response['updated_objects'] == populate_custom_fields_response['number_received']:
        print(f'Updated custom fields for article "{article_title}" sucessfully')
    else:
        print(f'Error updating article {article_title}')
        print(populate_custom_fields_response['error'])

else:
    print('No custom template id was found')





