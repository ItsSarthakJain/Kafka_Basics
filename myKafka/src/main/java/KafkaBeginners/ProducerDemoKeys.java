package KafkaBeginners;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        /// create Producer properties
        String bootstrapServers="127.0.0.1:9092";

       Logger logger=LoggerFactory.getLogger(ProducerDemoKeys.class);

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // create the producer

        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);

        //create a prodicer record
        for(int i=0;i<10;i++) {
            String topic="first_topic";
            String value="Hello world"+Integer.toString(i);

            String key=Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);


            // send data

            logger.info("Key: "+key);
            producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //  System.out.printf("\n\n\n\n\n\n" +"helll\n\n\n\n\n" );
                    if (e == null) {
                        // System.out.printf("\n\n\n\n\n\n" +"helll\n\n\n\n\n" );
                        logger.info("Recieved new metadata\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "TimeStamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing ", e);
                    }
                }
            }).get();
        }
       producer.flush();
       producer.close();

    }
}
