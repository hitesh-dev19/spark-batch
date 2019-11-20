package spark.commonService;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;


import spark.constants.CommonConstants;


public class CommonService {
	
	
	public AmazonS3 creates3Obj() {
		// credentials object identifying user for authentication
		// user must have AWSConnector and AmazonS3FullAccess for
		// this example to work
		AWSCredentials credentials = new BasicAWSCredentials(CommonConstants.ACCESS_KEY_ID,CommonConstants.ACCESS_SEC_KEY);

		// create a client connection based on credentials
		//AmazonS3 s3client = new AmazonS3Client(credentials);
		
		AmazonS3 s3client = AmazonS3ClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(Regions.US_EAST_2)
				  .build();
		
		return s3client;
	}
	
	

	public void getObj(AmazonS3 s3client)  {
	        String bucketName = CommonConstants.BUCKET_NAME;
	        String objectName = CommonConstants.BUCKET_FILE_PATH;

	        try {
	        S3Object s3object = s3client.getObject(bucketName, objectName);
	        S3ObjectInputStream inputStream = s3object.getObjectContent();
	        FileUtils.copyInputStreamToFile(inputStream, new File(CommonConstants.LOCAL_DOWNLOAD_PATH));
	        
	        
	        System.out.println("file copied to destination.");
	        }catch(Exception e) {
	        	e.printStackTrace();
	        }
	    }
	
	public static void createFolder(String bucketName, String folderName, AmazonS3 client,String SUFFIX) {
		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);

		// create empty content+
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + SUFFIX, emptyContent,
				metadata);

		// send request to S3 to create folder
		client.putObject(putObjectRequest);
	}

	/**
	 * This method first deletes all the files in given folder and than the folder
	 * itself
	 */

	public static void deleteFolder(String bucketName, String folderName, AmazonS3 client) {
		List fileList = client.listObjects(bucketName, folderName).getObjectSummaries();
		for (Object object : fileList) {
			S3ObjectSummary file = (S3ObjectSummary) object;
			client.deleteObject(bucketName, file.getKey());
		}
		client.deleteObject(bucketName, folderName);
		System.out.println("Folder Deleted");
	}
	     
}
	

