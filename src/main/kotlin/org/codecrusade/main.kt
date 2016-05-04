package org.codecrusade

import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.network.KafkaChannel
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.BasicConfigurator
import java.util.*
import java.util.logging.Logger

var logger = Logger.getGlobal()

fun main(args: Array<String>) {
    BasicConfigurator.configure();

    val topic = "my_strange_topic"

    val props = Properties()
    props.put("bootstrap.servers", "192.168.0.60:9092")
    props.put("group.id", "consumer-tutorial")
    props.put("key.deserializer", StringDeserializer::class.java.name )
    props.put("value.deserializer", StringDeserializer::class.java.name)
    //props.put("enable.auto.commit","false")
    props.put("auto.offset.reset","earliest")
    //props.put("metrics.num.samples","1")
    val consumer = KafkaConsumer<String,Any>(props)
    consumer.subscribe(listOf(topic))
    val mets = consumer.metrics()

    val pThread = Thread(Runnable {
        val props = Properties()
        props.put("bootstrap.servers", "192.168.0.60:9092")
        //props.put("zookeeper.connect", "192.168.0.60:2181")
        props.put("key.serializer", StringSerializer::class.java.name )
        props.put("value.serializer", StringSerializer::class.java.name)
        props.put("acks", "1")
        val producer = KafkaProducer<String,Any>(props)
        try {
            while(true) {
                producer.send(ProducerRecord(topic, "key", "{}"));
                Thread.sleep(200)
            }
        }
        finally {
            producer.close()
        }
    })
    pThread.start()

    try {
        while (true) {
            val records = consumer.poll(Long.MAX_VALUE);
            records.forEach {
                println("#{it.offset()} : #{it.value()}")
            }
        }
    } catch (e: WakeupException) {
        // ignore for shutdown
    } finally {
        pThread.join()
        consumer.close()
    }
}
