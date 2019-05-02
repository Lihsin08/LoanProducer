package com.adonmey

import java.io.File
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random, TimeZone}

import com.google.gson.{Gson, JsonObject}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.kafka.clients.producer._

object KafkaProducer {
  var config_file: Config= _
  var producerParams= new Properties()
  var topic:String = _
  var kafka_producer:KafkaProducer[String, String]= _


  def setKafkaProducerParams={
    topic = config_file.getString("kafka.topic")
    producerParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config_file.getString("kafka.bootstrap_servers"))
    producerParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config_file.getString("kafka.key.serializer"))
    producerParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config_file.getString("kafka.value.serializer"))
    producerParams.put(ProducerConfig.ACKS_CONFIG, config_file.getString("kafka.acks"))
    producerParams.put(ProducerConfig.RETRIES_CONFIG, config_file.getString("kafka.retries"))
    kafka_producer = new KafkaProducer[String, String](producerParams)
  }


  def cvsIterater(csvfile:String)={

    val File = new File(csvfile)
    val parser = CSVParser.parse(File, Charset.forName("UTF-8"), CSVFormat.DEFAULT)

    parser.iterator()

  }

  def publish_message(csvfile: String)={

    val gson:Gson = new Gson
    val messageIterator= cvsIterater(csvfile)


    while (messageIterator.hasNext) {
      val random = new Random
      // pack message in Json object
      val jsonObj:JsonObject = new JsonObject
      val message = messageIterator.next()

      //get current time
      val isoFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      isoFormat.setTimeZone(TimeZone.getTimeZone("CT"));
      val d = new Date()
      val timestamp = isoFormat.format(d)


      jsonObj.addProperty("loan_id", message.get(0))
      jsonObj.addProperty("active_status", message.get(1))
      jsonObj.addProperty("current_balance", message.get(2))
      jsonObj.addProperty("current_interest_rate", message.get(3))
      jsonObj.addProperty("days_delinquent", message.get(4))
      jsonObj.addProperty("delinquency_history_string", message.get(5))
      jsonObj.addProperty("period_of_payment", message.get(6))
      jsonObj.addProperty("timestamp", timestamp)

      // Serialize json object
      val jsonString:String = gson.toJson(jsonObj)
      println("Transaction Record: " + jsonString)

      // kafka producer send
      //https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
      //https://kafka.apache.org/11/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html
      val producerRecord = new ProducerRecord[String, String](topic, jsonString)
      kafka_producer.send(producerRecord, new callback)

      // mimic random message publish
      Thread.sleep(2000+ random.nextInt(1000))
      //Thread.sleep(100)


    }

    class callback extends Callback{
      def onCompletion(metadata:RecordMetadata, e:Exception): Unit ={
        if (e != null){
          System.out.println("Producer failed with exception" + e)
        }
        else{
          System.out.println("Record partition: " + metadata.partition + " offset: " + metadata.offset)
        }
      }

    }




  }



  def main(args: Array[String])={
    config_file = ConfigFactory.parseFile(new File(args(0)))
    setKafkaProducerParams

    val producer_file:String = config_file.getString("kafka.file")
    publish_message(producer_file)


  }


}
