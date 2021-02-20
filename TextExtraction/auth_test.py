def authtest():
	from google.cloud import storage
	storage_client=storage.Client.from_service_account_json('VisionAPI-d99a38a03511.json')
	#TODO: Correctly set the env variable for GOOGLE_APPLICATION_CREDENTIALS and the following code should work after setting up the env variable but for some reason it doesn't
	#storage_client=storage.Client()
	buckets=list(storage_client.list_buckets())
	print (buckets)

#Run the function in the same file (for now)
try:
	authtest() #returns the list of cloud storage buckets if auth is successful 
except Exception as e:
	print(e)
