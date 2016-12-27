package main.java.com.jkamal;

import com.amazonaws.services.kinesis.producer.*;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by joarderk on 12/27/16.
 */
public class TweetStreamProducer implements Runnable {
    private KinesisProducer kinesisProducer;
    private KinesisProducerConfiguration cfg;

    final AtomicLong completed = new AtomicLong(0);

    private void init() {
        cfg = KinesisProducerConfiguration.fromPropertiesFile("./src/main/resources/kpl.properties");
        kinesisProducer = new KinesisProducer(cfg);
    }

    @Override
    public void run() {
        // Initialization
        init();

        // Result handler
        final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                if (t instanceof UserRecordFailedException) {
                    Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
                    Main.log.error(String.format("Record failed to put - %s : %s", last.getErrorCode(), last.getErrorMessage()));
                }

                Main.log.error("Exception during put", t);
                System.exit(1);
            }

            @Override
            public void onSuccess(UserRecordResult result) {
                //Main.log.info("Successfully put ["+result.toString()+" ]");
                completed.getAndIncrement();
            }
        };

        // Progress updates
        Thread progress = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    // Here we're going to look at the number of user records put over a 60 seconds sliding window.
                    try {
                        for (Metric m : kinesisProducer.getMetrics("UserRecordsPut", 60)) {
                            if (m.getDimensions().size() == 1 && m.getSampleCount() > 0) {
                                Main.log.info("Aggregated record counts: "+Main.aggregatedRecords);
                                Main.log.info(String.format(
                                        "(Sliding 60 seconds) Avg put rate: %.2f per sec, " +
                                                "success rate: %.2f, failure rate: %.2f, total attempted: %d",
                                        m.getSum() / 15,
                                        m.getSum() / m.getSampleCount() * 100,
                                        (m.getSampleCount() - m.getSum()) / m.getSampleCount() * 100,
                                        (long) m.getSampleCount()));
                            }
                        }
                    } catch (Exception e) {
                        Main.log.error("Unexpected error getting metrics", e);
                        System.exit(1);
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        progress.start();

        // Put records
        Main.log.info("Initial size of the input queue: "+Main.tweets.size());

        while (true) {
            String tweet = null;
            String partitionKey = null;
            ByteBuffer record = null;

            try {
                if (Main.RANDOM.nextDouble() < 1e-5)
                    Main.log.info("[Randomly checking] Size of the tweet queue: "+Main.tweets.size());

                tweet = Main.tweets.take();
                partitionKey = Long.toString(System.currentTimeMillis()); //timetstamp

                record = ByteBuffer.wrap(tweet.getBytes("UTF-8"));
                ++Main.aggregatedRecords;

            } catch(Exception e) {
                e.printStackTrace();
            }

            ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(Main.STREAM, partitionKey, record);
            Futures.addCallback(f, callback);
        }
    }
}
