package BillingOps

import BillingOps.BillingLogic.generateAndExecuteFeeBatch

import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import QueryUtils.getBillingSummaries
import CsvUtils.{bqsToTrexRowList, writeToTrexCsv}
import com.farther.pubsub.kafka.ConfiguredKafkaProducer
import org.apache.kafka.clients.producer.KafkaProducer

object BillingMain extends App {

  // Load Billing Configs
  val billingConf = ConfigFactory
    .load("application.conf")
    .getConfig("Billing")
  val basePath = billingConf.getString("outFileBasePath")
  println(s"File base path is $basePath")
  val monthYear = "Jun-21"

  // Create the target path
  val outputFPath = basePath + "/Farther_Billing_File-" + monthYear + ".csv"
  val bqsFut = getBillingSummaries
  val bqs = Await.result(bqsFut, 60.seconds)

  val bqsTrexRows = bqsToTrexRowList(bqs, monthYear)
  println(s"Produced output length is ${bqsTrexRows.length}")
  println(s"Outputting billing file to $outputFPath")
  writeToTrexCsv(bqsTrexRows, outputFPath)
  println("Operation Complete")

}

object BillingMainAutomated extends App {
  implicit val kafkaProducer: KafkaProducer[String, String] = new ConfiguredKafkaProducer().producer
  implicit val producerFuncs: KafkaProducerFuncs = new KafkaProducerFuncs()

  // Load Billing Configs
  val billingConf = ConfigFactory
    .load("application.conf")
    .getConfig("Billing")
  val basePath = billingConf.getString("outFileBasePath")
  println(s"File base path is $basePath")
  val monthYear = "Jun-21"

  val bqsFut = getBillingSummaries
  val bqs = Await.result(bqsFut, 60.seconds).toList

  val executeFut = generateAndExecuteFeeBatch(bqs, KAFKA_TEST_TOPIC)
  Thread.sleep(5)

  println("Operation Complete")

}
