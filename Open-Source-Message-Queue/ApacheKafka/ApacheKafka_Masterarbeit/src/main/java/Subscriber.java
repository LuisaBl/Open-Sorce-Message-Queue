
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Subscriber {
    public static void main(String[] args) {
        //Create logger for class
        final Logger logger = LoggerFactory.getLogger(Subscriber.class);
        //Create variables for Stings
        final String bootstrapServer = "127.0.0.1:9092";
        final String consumerGroupID = "java-group-consumer";
        //
        Properties p = new Properties();
        //Connection Server
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        //Deserializer Key/Value
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //GroupID
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        //GroupStream
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);

        //Subscribe to Topics
        consumer.subscribe(Arrays.asList("QUEUE"));
        // POll and Consume records
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord record: records){
                logger.info("Received new record: \n" +
                        "Key: " + record.key() +","+
                        "value: " + record.value() + ","+
                        "Topic: " + record.topic() + ","+
                        "Partition: "+ record.partition() +"," +
                        "Offset: " + record.offset() + "\n");
            }
        }
    }
}
