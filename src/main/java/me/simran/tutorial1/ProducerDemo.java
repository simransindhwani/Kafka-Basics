package me.simran.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootStrapServer = "127.0.0.1:9092";
        /**Steps to writing Producer Program
         * 1. Creating Producer Properties
         * 2. Creating a Producer
         * 3. Sending Data
         */

        /** Step 1: Setting the Properties */
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);

        /**kEY AND VALUE tells what kind of data has been sent over - here String */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /** Step 2: Creating a Producer
         * <String, String> suggests the Key-Value Pair
         **/
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        /**Creating a Producer Record*/
        ProducerRecord<String, String> record =
                new ProducerRecord<>("first_topic", "Hello, this is my first data!");

        /**Step 3: Sending data using Producer - Async Operation {Output cannot be seen} */
        producer.send(record);

        /**Flush and close, waits until that data is sent */
        producer.flush();
        producer.close();

    }
}
