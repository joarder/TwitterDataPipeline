package main.java.com.jkamal;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;

/**
 * Created by joarderk on 12/1/16.
 */
public class Config {
    private static String consumer_key;
    private static String consumer_secret;
    private static String access_token_key;
    private static String access_token_secret;

    public static String getConsumer_key() {
        return consumer_key;
    }

    private static void setConsumer_key(String consumer_key) {
        Config.consumer_key = consumer_key;
    }

    public static String getConsumer_secret() {
        return consumer_secret;
    }

    private static void setConsumer_secret(String consumer_secret) {
        Config.consumer_secret = consumer_secret;
    }

    public static String getAccess_token_key() {
        return access_token_key;
    }

    private static void setAccess_token_key(String access_token_key) {
        Config.access_token_key = access_token_key;
    }

    public static String getAccess_token_secret() {
        return access_token_secret;
    }

    private static void setAccess_token_secret(String access_token_secret) { Config.access_token_secret = access_token_secret; }

    public static void readTwitterConfig() {
        Configurations configs = new Configurations();
        File propertiesFile = new File("twitter.properties");

        try {
            PropertiesConfiguration config = configs.properties(propertiesFile);

            setConsumer_key((String)config.getProperty("consumer_key"));
            setConsumer_secret((String)config.getProperty("consumer_secret"));
            setAccess_token_key((String)config.getProperty("access_token_key"));
            setAccess_token_secret((String)config.getProperty("access_token_secret"));

        } catch(ConfigurationException cex) {
            cex.printStackTrace();
            Main.log.error("Can not read Twitter application configuration file!!!");
        }
    }

    public static void readKinesisConfig() {
        Configurations configs = new Configurations();
        File propertiesFile = new File("kinesis.properties");

        try {
            PropertiesConfiguration config = configs.properties(propertiesFile);
            Main.REGION = ((String)config.getProperty("AwsRegion"));
            Main.STREAM = ((String)config.getProperty("KinesisStream"));
            Main.SHARD_COUNT = (Integer.parseInt((String) config.getProperty("ShardCount")));
            Main.APP = ((String)config.getProperty("ApplicationName"));

        } catch(ConfigurationException cex) {
            cex.printStackTrace();
            Main.log.error("Can not read Kinesis configuration file!!!");
        }
    }
}