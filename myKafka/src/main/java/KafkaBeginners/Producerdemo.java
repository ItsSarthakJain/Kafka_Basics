package KafkaBeginners;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;

public class Producerdemo {
    public static void main(String[] args) {

        /// create Producer properties
        String bootstrapServers="127.0.0.1:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // create the producer

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //create a prodicer record

        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","hello world");


        // send data

        producer.send(record);
        producer.flush();
        producer.close(Duration.ofMillis(1000));

    }
}
