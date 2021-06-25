package me.simran.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        /**Creating a logger for the class*/
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

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

            String topic = "first_topic";
            String value = "Hello, this is my first data from: "+Integer.toString(i);

            /**Generating a Key */
            String key = "id_"+ Integer.toString(i);

            /**Creating a Producer Record*/
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic,key, value);

            logger.info("Key:"+key);

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
            }).get(); /**To make the loop run synchronously Key - 0,1,2....* All the keys go in the same partition. (It is guaranteed) */
        }

        /**Flush and close, waits until that data is sent */
        producer.flush();
        producer.close();

    }
}
