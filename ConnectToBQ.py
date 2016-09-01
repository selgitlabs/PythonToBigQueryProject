import argparse
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


def get_authenticate():
	print '*******Connecting to Bigquery**********'
	credentials = GoogleCredentials.get_application_default()
	bigquery_service = discovery.build('bigquery', 'v2', credentials=credentials)
	return bigquery_service

	
def execute_query(bigquery_service, project_id, query_str):
	print '*******Executing Query**********'
	print 'ProjectId: {0} query: {1}'.format(project_id, query_str)
	query_request = bigquery_service.jobs()
	query_data = {'query': query_str}
	query_response = query_request.query(projectId=project_id, body=query_data).execute()
	for row in query_response['rows']:
		print row


def main(project_id, query_str):
	bigquery_service = get_authenticate()
	execute_query(bigquery_service, project_id, query_str)
	
	
if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--projectid', help='Enter projectId')
	parser.add_argument('--query', help='Your Big Query.')
	args = parser.parse_args()
	#query_str = 'SELECT * FROM [hd-data-dev:cart.adjustments] LIMIT 2;';
	query_str = args.query
	project_id = args.projectid
	main(project_id, query_str)
