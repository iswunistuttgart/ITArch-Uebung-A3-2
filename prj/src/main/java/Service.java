import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Created by Matthias Milan Strljis on 26.10.2017.
 * Copyright ISW Univerisity of Stuttgart
 */
public class Service {
    public static String TEAMCONSANT = "TEAM-";
    public static String GENERAL_MESSAGE = "GENERAL-CHANNEL";
    public static String BEST_QRCODE = "BEST-CODE-CHANNEL-TEAM-";
    public static float CURRENT_BEST_MESSAGE = 0;
    public static QRMessage BEST_MESSAGE = null;
    public static org.apache.kafka.clients.producer.Producer<String, String> producer;




    public static void main(String args[]){
        TEAMCONSANT += System.getProperty("TEAM","TESTTEAM");
        BEST_QRCODE += System.getProperty("TEAM","TESTTEAM");
        Gson gson = new Gson();
        Properties propsProducer = new Properties();
        System.out.println("Setup Enivorment TEAM is: " + TEAMCONSANT);
        propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("kafka","193.196.38.196:9092"));
        propsProducer.put("acks", "all");
        propsProducer.put("retries", 0);
        propsProducer.put("batch.size", 16384);
        propsProducer.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        propsProducer.put("linger.ms", 1);
        propsProducer.put("buffer.memory", 33554432);
        propsProducer.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        propsProducer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propsProducer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(propsProducer);

        new Thread(() -> {
            try {
                while (true){
                    if(BEST_MESSAGE != null){
                        producer.send(new ProducerRecord<String, String>(BEST_QRCODE, gson.toJson(BEST_MESSAGE))).get();
                        System.out.println("Send Message to ["+BEST_QRCODE + "] value: " + gson.toJson(BEST_MESSAGE));

                    }
                    Thread.sleep(1000);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }).start();

        Properties props = new Properties();
        props.put("bootstrap.servers", System.getProperty("kafka","193.196.38.196:9092"));
        props.put("group.id", "Consumer-" + UUID.randomUUID());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(GENERAL_MESSAGE));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                try{
                    QRMessage message = gson.fromJson(record.value(), QRMessage.class);
                    System.out.println("Nachricht erhalten: " + record.value());
                    if(message.getTeam().equals(args[0])){
                        producer.send(new ProducerRecord<String, String>(TEAMCONSANT, gson.toJson(message))).get();
                    }
                    if(message.getSpeed() > CURRENT_BEST_MESSAGE){
                        producer.send(new ProducerRecord<String, String>(BEST_QRCODE, gson.toJson(message))).get();
                        CURRENT_BEST_MESSAGE = message.getSpeed();
                        BEST_MESSAGE = message;
                    }
                }catch (Exception e){
                    System.out.println("Nachricht konnte nicht serialisiert werden!");
                }

            }
        }
    }

public static class QRMessage{
    private String QRCode = "Kein CODE";
    private float Speed = 0;
    private String Team = "Kein CODE";

    public QRMessage() {
    }

    public String getQRCode() {
        return QRCode;
    }

    public void setQRCode(String QRCode) {
        this.QRCode = QRCode;
    }

    public float getSpeed() {
        return Speed;
    }

    public void setSpeed(float speed) {
        Speed = speed;
    }

    public String getTeam() {
        return Team;
    }

    public void setTeam(String team) {
        Team = team;
    }
}


}
