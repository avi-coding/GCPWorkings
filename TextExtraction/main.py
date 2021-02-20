import json
import re
from google.cloud import vision
from google.cloud import storage 

def async_detect_document(gcs_source_uri, gcs_destination_uri):
    # Setting up the mime type to pdf first. We will explore this further with image types too'
    mime_type = 'application/pdf'
    #mime_type = 'image/tiff'

    # batch size sets up number of pages within a pdf that should be grouped together 
    batch_size = 100

    # The workflow of text extraction we are using would as a first thing, annotate the pdf wherever there is text in the pdf or image.
    # Remember to add the service account credentials explicitly if your env variable is not set (which is the case with me)
    vision_client = vision.ImageAnnotatorClient.from_service_account_json('VisionAPI-d99a38a03511.json')

    feature = vision.Feature(
        type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

    #These are important two lines as the first one tells the vision API the location of our document and the second one tells it the mime type of the document 
    gcs_source = vision.GcsSource(uri=gcs_source_uri)
    input_config = vision.InputConfig(
        gcs_source=gcs_source, 
        mime_type=mime_type)

    #The below two lines are providing the destination of the output (JSON files) with annotations as well as the config parameteres (such as batch size)
    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, 
        batch_size=batch_size)
    print(gcs_destination)
    #The statement below sets up the asynchronous request with our input and output configs created above. We will use this in making an asynch request to annotate our file 
    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], 
        input_config=input_config,
        output_config=output_config)

    #This is the part where we ask the vision api to initiate an asynch request to annotate the file
    operation = vision_client.async_batch_annotate_files(
        requests=[async_request])

    #We run the operation to annotate and set up a timeout to ensure it doesn't keep on running. This part needs more work on how to run batches of documents in one go...
    print('Waiting for the operation to finish.')

    operation.result(timeout=420)

    print('Operation finished')

def write_to_text(gcs_destination_uri):
    # This function can only be run if the function above (produce JSON files with annotations) has completed successfully. 
    # This function takes the input parameter of the JSON files location where the annotations are saved.
    # I have assumed this to be in a folder within my gcs bucket but it can be elsewhere too.
    storage_client = storage.Client.from_service_account_json('VisionAPI-d99a38a03511.json')

    # Get the JSON files from the input parameter (in my case, this is the gcp bucket folder)
    match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
    bucket_name = match.group(1)
    print(bucket_name)
    prefix = match.group(2)
    print(prefix)

    bucket = storage_client.get_bucket(bucket_name)

    # We then get the list of JSON files to process
    blob_list = list(bucket.list_blobs(prefix=prefix))
    
    print('Output files:')

    #Create a text file to output the transcript within the pdf/image
    #TODO: Ensure this is in a try except finally block
    transcription = open("transcript.txt", "w")

    for blob in blob_list:
        
        print(blob.name)

    # Process the first output file from gcp bucket.The first response contains first two pages of the input file.
    for n in  range(len(blob_list)):
        output = blob_list[n]

        #For some reason, the folder itself is coming up as the first file, which is incorrect and hence the below hack. 
        if n > 0:
        	
        	json_string = output.download_as_string()
        	response = json.loads(json_string)


        	# For each response captured within the JSON annotated file.
	        for m in range(len(response['responses'])):

	            page_response = response['responses'][m]

	            try:
	                annotation = page_response['fullTextAnnotation']
	            except(KeyError):
	                print("No annotation for this page.")

	            # Here we print the full text. However, the JSON response contains more information such as:
	            # annotation/pages/blocks/paragraphs/words/symbols including confidence scores and bounding boxes
	            print('Full text:\n')
	            print(annotation['text'])
	            
	            with open("transcript.txt", "a+", encoding="utf-8") as f:
	                f.write(annotation['text'])

        
#Here is where we call the two functions above. We can do this from any other python file or program. 

#async_detect_document("gs://text_extract_22/Avi Aggarwal Morpheus Personal Services Company Agreement  - signed (1).pdf", "gs://text_extract_22/AnnotatedJSON/")
#write_to_text("gs://text_extract_22/AnnotatedJSON")