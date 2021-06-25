package me.simran.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {

        String bootStrapServers = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
        String groupId = "My-application";
        String topic = "first_topic";

        /** Creating Consumer Properties */
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /** Creating a consumer */

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        /** Assign and Seek are used to replay data or fetch a specific message */
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        /**Seek */
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberofMessagesToRead =5;
        boolean keepOnReading = true;
        int numberOfMessagesRead =0;

        /** Poll for new Data */
        while(keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record: records){
                numberOfMessagesRead++;
                logger.info("Key: "+record.key()+" Value: "+record.value());
                logger.info("Partition: "+record.partition()+" Offset: "+record.offset());
                if(numberOfMessagesRead>=numberofMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
