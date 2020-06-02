package com.demo.kafka

  import java.util.Properties

  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

  class Producer{
    val newArgs = Array("20","cricket","localhost:9092")
    val events = newArgs(0).toInt
    val topic = newArgs(1)
    val brokers = newArgs(2)
    def configureKafkaProducer():Properties={
      val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("client.id", "producer")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props
    }

def sendMessage(): Unit ={
 val props= configureKafkaProducer()
  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()
  for (nEvents <- Range(0, events)) {
    val key = "IPL " + nEvents.toString
    val msg = "crickettest"
    val data = new ProducerRecord[String, String](topic, key, msg)

    //async
    //producer.send(data, (m,e) => {})
    //sync
    producer.send(data)

  }
  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}




  }
object  Producer extends  App{
  var producer=new Producer();
  producer.configureKafkaProducer()
  producer.sendMessage()
}
