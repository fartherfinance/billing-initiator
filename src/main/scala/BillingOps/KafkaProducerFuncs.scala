package BillingOps

import java.util.concurrent.Future

import com.farther.grecoevents.Event

import io.circe.generic.extras.auto._
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import com.farther.grecoevents.codecs.EventCodec._


class KafkaProducerFuncs(implicit producer: KafkaProducer[String, String]) {
  def sendMessage(topic: String, messageValue: String, messageKey: Option[String]=None): Future[RecordMetadata] = {
    val record = messageKey match {
      case Some(messageKey) => new ProducerRecord[String, String](topic, messageKey, messageValue)
      case None => new ProducerRecord[String, String](topic, messageValue)
    }

    println(s"Sending message to Kafka:\n$messageValue")
    producer.send(record)
  }
}

object KafkaProducerFuncs {
  def apply(implicit producer: KafkaProducer[String, String]): KafkaProducerFuncs = new KafkaProducerFuncs()

  def publishToKafka(event: Event, topic: String)(implicit kafkaProducer: KafkaProducerFuncs): Future[RecordMetadata] = {
    kafkaProducer.sendMessage(topic = topic, messageValue = event.asJson.noSpaces)
  }

  def publishStringToKafka(str: String, topic: String)(implicit kafkaProducer: KafkaProducerFuncs): Future[RecordMetadata] = {
    kafkaProducer.sendMessage(topic = topic, messageValue = str)
  }
}