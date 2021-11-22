package service;

import deserializer.VendaDeserializer;
import model.Venda;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class ProcessadorVendas {
  public static void main(String[] args) throws InterruptedException {

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VendaDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-processamento");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Para pegar as mensagens que ja existem desde o inicio.
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
    try (KafkaConsumer<String, Venda> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(Arrays.asList("venda-ingresso"));

      while (true) {
        ConsumerRecords<String, Venda> vendas = consumer.poll(Duration.ofMillis(200));
        for (ConsumerRecord<String, Venda> consumerRecord : vendas) {
          Venda venda = consumerRecord.value();
          if (new Random().nextBoolean()) {
            venda.setStatus("Aprovada");
          } else {
            venda.setStatus("Reprovada");
          }
          Thread.sleep(500);
          System.out.println(venda);
        }
      }
    }
  }

}
