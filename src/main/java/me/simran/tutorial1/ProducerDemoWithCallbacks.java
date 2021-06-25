package me.simran.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {
    public static void main(String[] args) {

        /**Creating a logger for the class*/
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);

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

        for(int i=0; i<10;i++){
            /**Creating a Producer Record*/
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", "Hello, this is my first data!");

            /**Step 3: Sending data using Producer - Async Operation {Output cannot be seen} */
            producer.send(record, new Callback() {

                /**Setting callback to see more information like topics, partitions, timestamp, offset */
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        /** No Exception Throw - True case, It is generated each time for every data*/
                        logger.info("New Information Received \n"+"Topic: "+recordMetadata.topic()+
                                "\n Partitions: "+recordMetadata.partition()+
                                "\n Offset: "+recordMetadata.offset()+
                                "\n TimeStamp:"+recordMetadata.hasTimestamp());
                    }
                    else{
                        /** Exception Thrown */
                        logger.error("Error has been encountered.", e);
                    }
                }
            });
        }

        /**Flush and close, waits until that data is sent */
        producer.flush();
        producer.close();

    }
}
