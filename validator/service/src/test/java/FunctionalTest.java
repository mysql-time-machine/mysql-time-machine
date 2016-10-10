import com.booking.validator.data.constant.ConstDataPointerFactory;
import com.booking.validator.data.DataPointerFactory;
import com.booking.validator.service.DataPointers;
import com.booking.validator.service.Service;
import com.booking.validator.service.Validator;
import com.booking.validator.service.protocol.DataPointerDescription;
import com.booking.validator.service.protocol.ValidationTaskDescription;
import com.booking.validator.service.task.TaskSupplier;
import com.booking.validator.service.task.kafka.KafkaValidationTaskDescriptionSupplier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * Created by psalimov on 10/3/16.
 */
public class FunctionalTest {

    private static final String TOPIC = "topic";

    private KafkaServerStartable kafka;
    private Service producer;

    private Supplier<ValidationTaskDescription> getSupplier(){

//        Map<String, String> constStorageDescription = new HashMap<>();
//        constStorageDescription.put("type", "const");
//
//
//        Map<String, String> constKeyDescription = new HashMap<>();
//        constKeyDescription.put("value", "{ \"a\": 1, \"b\" : 2 }");
//
//        DataPointerDescription source = new DataPointerDescription(constStorageDescription,constKeyDescription);
//        new ValidationTaskDescription(source,source);
//
//        return ()->{
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                return new ValidationTaskDescription(source,source);
//            };


        Map<String,String> configuration = new HashMap<>();

        configuration.put("bootstrap.servers","127.0.0.1:9092");
        configuration.put("group.id","group0");
        configuration.put("client.id","consumer0");

        Properties properties = new Properties();

        configuration.entrySet().stream().forEach( x -> properties.setProperty(x.getKey(), x.getValue()) );

        return KafkaValidationTaskDescriptionSupplier.getInstance( TOPIC, properties );

    }

    private KafkaServerStartable getKafkaServer() throws Exception {

        ZooKeeperServerMain zookeeper = new ZooKeeperServerMain();
        ServerConfig zooCfg = new ServerConfig();
        zooCfg.parse(new String[]{"2181","/tmp/zk"});

        new Thread(() -> {
            try {
                zookeeper.runFromConfig(zooCfg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();


        Properties properties = new Properties();

        properties.put("port", 9092);
        properties.put("zookeeper.connect", "127.0.0.1:2181");


        KafkaConfig config = new KafkaConfig(properties);

        return new KafkaServerStartable(config);

    }

    private String task() throws JsonProcessingException {

        Map<String, String> constStorageDescription = new HashMap<>();
        constStorageDescription.put("type", "const");


        Map<String, String> constKeyDescription = new HashMap<>();
        constKeyDescription.put("value", "{ \"a\": 1, \"b\" : 2 }");

        DataPointerDescription source = new DataPointerDescription(constStorageDescription,constKeyDescription);

        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(new ValidationTaskDescription(source,source));

    }

    private Service getKafkaProducer() {

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        KafkaProducer<String,String> kp = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());

        Thread t = new Thread(()->{

            for(;;) {

                String task = null;
                try {
                    task = task();
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                ProducerRecord<String, String> r = new ProducerRecord(TOPIC, task);

                kp.send(r);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        return new Service() {
            @Override
            public void start() {
                t.start();
            }

            @Override
            public void stop() {

            }
        };
    }

    @Before
    public void prepare() throws Exception {
        kafka = getKafkaServer();
        kafka.startup();

        producer = getKafkaProducer();
        producer.start();

    }

    @After
    public void cleanup(){
        if (kafka != null) kafka.shutdown();
        if (producer != null) producer.stop();
    }

    @Test
    public void test(){

        Map<String, DataPointerFactory> factories = new HashMap<>();
        factories.put( "const", new ConstDataPointerFactory() );

        Validator validator = new Validator( new TaskSupplier( getSupplier(), new DataPointers( factories )), x-> System.out.println(x.isOk()), System.out::println );

        validator.start();

        try {
            Thread.sleep(1000*60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
