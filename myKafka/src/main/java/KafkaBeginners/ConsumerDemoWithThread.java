package KafkaBeginners;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }



    private void run()
    {
        Logger logger= LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootStrapConfig="127.0.0.1:9092";
        String grpid="my_st_application";
        String topic="first_topic";
        CountDownLatch latch=new CountDownLatch(1);
        logger.info("Creating the consumer");

        Runnable myConsumerRunnable=new ConsumerRunnable(latch,topic,bootStrapConfig,grpid);

        Thread mythread=new Thread(myConsumerRunnable);
        mythread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("RECIEVED SHUTDOWN SIGNAL");
             ((ConsumerRunnable) myConsumerRunnable).shutdown();
        }));

        try {
            latch.await();
        }
        catch (InterruptedException e)
        {
            logger.info("Application is intruppted");
        }
        finally {
            logger.info("Shuting the application down");
        }


    }

    public class ConsumerRunnable implements Runnable
    {
        private KafkaConsumer<String,String> consumer;
        private CountDownLatch latch;
        Logger logger= LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch, String topic, String bootStrapConfig, String grpid )
        {
            this.latch=latch;
            Properties properties=new Properties();
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapConfig);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,grpid);

            consumer=new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run()
        {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        logger.info("Key " + record.key() + " Value " + record.value() + "\n");
                        logger.info("Partition " + record.partition() + " Offset " + record.offset());
                    }
                }
            }
            catch (WakeupException e)
            {
                logger.info("Recieved shutdown signal");
            }
            finally {
                consumer.close();
                latch.countDown();
            }
        }
        public void shutdown()
        {
            consumer.wakeup();
        }
    }


}

