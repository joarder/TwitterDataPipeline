package main.java.com.jkamal;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;

/**
 * Created by joarderk on 12/27/16.
 */
public class TweetCrawler extends Thread {

    BlockingQueue<String> msgQueue;
    BlockingQueue<Event> eventQueue;
    Authentication auth;
    ClientBuilder builder;

    TweetCrawler() {
        super("Tweet Crawler");
    }

    @Override
    public void run() {
        // Filtering
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        //endpoint.trackTerms(Lists.newArrayList("*"));

        // Authenticate via OAuth
        Authentication auth = new OAuth1(
                Config.getConsumer_key(),
                Config.getConsumer_secret(),
                Config.getAccess_token_key(),
                Config.getAccess_token_secret());

        // Build a hosebird client
        ClientBuilder builder = new ClientBuilder()
                .name("tweet-crawler-client")
                .hosts(Constants.STREAM_HOST)
                .authentication(auth)
                .endpoint(new StatusesSampleEndpoint())
                .processor(new StringDelimitedProcessor(Main.tweets));


        Client client = builder.build();
        client.connect();

        try {
            while (!client.isDone()) {
                // Do nothing
                // Our tweet stream producer will run on a separate thread and prepare the record to be inserted into Kinesis Stream
            }
        } catch(Exception e) {
            e.printStackTrace();

        } finally {
            client.stop();
        }
    }
}
