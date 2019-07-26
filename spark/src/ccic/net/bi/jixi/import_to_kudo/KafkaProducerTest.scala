package bmsoft.ccic.adhoc.main

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducerTest {

  def main(args: Array[String]): Unit = {
    // Set up client Java properties
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node13:9092,node14:9092,node15:9092,node16:9092,node17:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.ACKS_CONFIG, "1")
    props.setProperty("security.protocol","SASL_PLAINTEXT")
    props.setProperty("sasl.mechanism","GSSAPI")
    props.setProperty("sasl.kerberos.service.name","kafka")

    val producer = new KafkaProducer[String, String](props)

    val data = new ProducerRecord[String, String]("TEST","1", "Hello, Kafka!")
    producer.send(data)
    producer.close()
  }

}
