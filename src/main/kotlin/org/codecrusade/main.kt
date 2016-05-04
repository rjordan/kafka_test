package org.codecrusade

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.BasicConfigurator

fun main(args: Array<String>) {
    BasicConfigurator.configure();

    val topic = "test"

    val cThread = Thread(Runnable {
        consumerThread(topic)
    })
    cThread.start()

    val pThread = Thread(Runnable {
        producerThread(topic)
    })
    pThread.start()

    while (true) {
    }
    //    pThread.join()
    //    cThread.join()
}

private fun consumerThread(topic: String) {
    val consumer = KafkaConsumer<String, Any>(mapOf(
            "bootstrap.servers" to "192.168.0.60:9092",
            "group.id" to "consumer-tutorial",
            "key.deserializer" to StringDeserializer::class.java.name,
            "value.deserializer" to StringDeserializer::class.java.name,
            "auto.offset.reset" to "earliest"
    ))
    consumer.subscribe(listOf(topic))
    try {
        while (true) {
            val records = consumer.poll(Long.MAX_VALUE);
            records.forEach {
                println("#{it.offset()} : #{it.value()}")
            }
        }
    } finally {
        consumer.close()
    }
}

private fun producerThread(topic: String) {
    val producer = KafkaProducer<String, Any>(mapOf(
            "bootstrap.servers" to "192.168.0.60:9092",
            "key.serializer" to StringSerializer::class.java.name,
            "value.serializer" to StringSerializer::class.java.name,
            "acks" to "1"
    ))
    try {
        while (true) {
            producer.send(ProducerRecord(topic, "key", "{\"FOO\":\"BAR\"}"));
            Thread.sleep(200)
        }
    } finally {
        producer.close()
    }
}
