package csw.spark.streaming


import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object Test {
    def main(args: Array[String]): Unit = {
        val props = new Properties()
        props.put("bootstrap.servers", "10.1.11.153:9192")

        props.put("acks", "all")
        props.put("retries", "0")
        props.put("batch.size", "16384")
        props.put("linger.ms", "1")
        props.put("buffer.memory", "33554432")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val kafkaProducer = new KafkaProducer[String, String](props)

        Source.fromFile("E:\\workspace\\other\\udap-pipeline-ml\\src\\test\\resources\\iris.csv").getLines().foreach(line => {
            println(line)
            kafkaProducer.send(new ProducerRecord[String, String]("iris", line))
            Thread.sleep(1000)
        })
        //kafkaProducer.close();
    }

}
