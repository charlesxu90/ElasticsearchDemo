package com.mininglamp.elasticsearch;
/*
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
*/
import org.json.JSONObject;
/**
 * Created by charles on 1/28/16.
 */

public class ElasticSearchTest {

    //private static final Logger LOGGER = LoggerFactory.getLogger(CustomerProfileLoad.class);
/*
    public static String process(String input) throws Exception{
        String[] fields = input.split(",");

        JSONObject customer_profile = new JSONObject();
        customer_profile.append("username", fields[8]);
        customer_profile.append("language", fields[10]);
        customer_profile.append("locale", fields[20]);
        customer_profile.append("country", fields[4]);
        customer_profile.append("jurisdiction", fields[0]);
        customer_profile.append("dateOfBirth", fields[7]);

        JSONObject header = new JSONObject();
        header.append("eventType", "CustomerRegistrationEvent");
        header.append("customerId", fields[16]);
        header.append("timestamp", fields[5]);
        customer_profile.append("header", header);

        return customer_profile.toString();
    }
*/
/*
    public static void main(String[] args) throws Exception {

        try {
            String customerProfileInput = "hdfs:///user/jinjia/CustomerRegistrationEvent.txt";
            SparkConf conf = new SparkConf().setAppName("Customer Profile Load").set("es.nodes", "localhost").set("es.port", "9200");
            conf.set("es.mapping.id", "header.customerId");
            JavaSparkContext sc = new JavaSparkContext(conf);
            JavaRDD<String> rawCustomerProfile = sc.textFile(customerProfileInput);
            JavaRDD<String> customerProfile = rawCustomerProfile.map(process);

            JavaEsSpark.saveJsonToEs(customerProfile, "testcustomerprofile/unibet");
//            JavaPairRDD<String, ImmutableOpenMap<String, String>> tempPairRDD = new GenerateCustomerProfileESTuple()
//                    .process(customerProfile);
//            JavaEsSpark.saveToEsWithMeta(tempPairRDD, "testcustomerprofile/unibet");

        } catch (Exception e) {
            // LOGGER.error("Encountered exception when processing the CustomerProfile [{}]", e.getMessage());
            System.err.println("Encountered exception when processing the CustomerProfile [{}]" + e.getMessage());
            throw e;
        }

    }
    */
}
