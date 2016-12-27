package main.java.com.jkamal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by joarderk on 12/27/16.
 */
public class Main {

    public static Logger log = LoggerFactory.getLogger(Main.class);
    public static BlockingQueue<String> tweets;

    static String REGION = null;
    static String STREAM = null;
    static String APP = null;
    static int SHARD_COUNT = 0;

    static Random RANDOM;
    public static int generatedRecords = 0;
    public static int producedRecords = 0;
    public static int aggregatedRecords = 0;
    public static int deaggregatedRecords = 0;
    public static int consumedRecords = 0;

    public static void main( String[] args ) throws Exception {
        // Initialization
        RANDOM = new Random();
        RANDOM.setSeed(0);
        tweets = new LinkedBlockingQueue<String>();

        // Reading configurations
        Config.readTwitterConfig();
        Config.readKinesisConfig();

        // Create the Kinesis Stream
        KinesisStreamManager.createStream();

        // Start crawling the tweets
        Thread tweetCrawler = new Thread(new TweetCrawler());
        tweetCrawler.setName("tweet-crawler");
        tweetCrawler.start();

        // Start the stream producer and streaming records to Kinesis Streams using KPL Aggregation
        Thread tweetStreamProducer = new Thread(new TweetStreamProducer());
        tweetStreamProducer.setName("tweet-stream-producer");
        tweetStreamProducer.start();
    }
}
