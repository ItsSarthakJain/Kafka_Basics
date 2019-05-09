package KafkaBeginners;

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

public class ConsumerSeekAssign {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumerSeekAssign.class.getName());

        String bootStrapConfig="127.0.0.1:9092";
        String topic="first_topic";
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapConfig);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        // assigning the partition to read from
        Long offsetToReadFrom=15L;
        TopicPartition partitionToReadFrom=new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partitionToReadFrom));
        //seeking to the offset or we can say the message we are goiing to read from

        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMessagesToRead=5;
        int numberOfMessagesReadSoFar=0;
        boolean keepOnReading=true;

        while(keepOnReading)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record :records)
            {
                numberOfMessagesReadSoFar++;
                logger.info("Key "+record.key()+" Value "+record.value()+"\n");
                logger.info("Partition "+record.partition()+" Offset "+record.offset() );
                if(numberOfMessagesReadSoFar>=numberOfMessagesToRead)
                {
                    keepOnReading=false;
                    break;
                }
            }
        }
    }
}
