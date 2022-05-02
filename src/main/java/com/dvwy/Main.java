package com.dvwy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import java.net.*;
import java.util.Properties;

public class Main {
    private final static int UDP_PORT = 4843;
    private final static Gson gson = new Gson();
    private final static String TOPIC_NAME = "forza-motorsport-raw";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws Exception {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.ACKS_CONFIG, 0);
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        DatagramSocket ds = new DatagramSocket(UDP_PORT);
        while (true) {
            byte[] receive = new byte[323];
            DatagramPacket dp = new DatagramPacket(receive, receive.length);
            ds.receive(dp);
            ForzaData forzaData = new ForzaData(dp.getData());
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, gson.toJson(forzaData));
            producer.send(record);
        }
    }
}