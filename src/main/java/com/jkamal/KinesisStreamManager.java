package main.java.com.jkamal;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;

public class KinesisStreamManager {
	static AmazonKinesisClient kinesisClient;
	
	private static void init() throws Exception {
        AWSCredentials credentials = null;
        AWSCredentialsProvider credentialsProvider = null;
        
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();

        } catch (AmazonClientException e) {
            Main.log.info("Unable to obtain credentials from the IMDS, trying classpath properties", e);
        }

        kinesisClient = new AmazonKinesisClient(credentials);
    }
	
    private static void waitForStreamToBecomeAvailable(String streamName) throws InterruptedException {
        Main.log.info("Waiting for <"+streamName+"> to become ACTIVE ...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(streamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                Main.log.info("\t- current state: "+streamStatus);
                
                if ("ACTIVE".equals(streamStatus))
                    return;

            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("Stream <"+streamName+"> never became active!!!"));
    }
    
    public static void createStream() throws Exception {
    	Main.log.info("Checking the status of Kinesis Stream ["+Main.STREAM+"] ...");
    	
        init();

        // Describe the stream and check if it exists
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(Main.STREAM);
        
        try {
            StreamDescription streamDescription = kinesisClient.describeStream(describeStreamRequest).getStreamDescription();
            Main.log.info("Stream <"+Main.STREAM+"> is in "+streamDescription.getStreamStatus()+" status.");

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
            	Main.log.info("Stream is being deleted. Exiting ...");
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE
            if (!"ACTIVE".equals(streamDescription.getStreamStatus()))
                waitForStreamToBecomeAvailable(Main.STREAM);
            
        } catch (ResourceNotFoundException ex) {
        	Main.log.info("Stream <"+Main.STREAM+"> does not exist. Creating it now ...");

            // Create a stream. The number of shards determines the provisioned throughput
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(Main.STREAM);
            createStreamRequest.setShardCount(Main.SHARD_COUNT);
            kinesisClient.createStream(createStreamRequest);

            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(Main.STREAM);
        }
    }
}
