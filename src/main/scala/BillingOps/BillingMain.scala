package BillingOps

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import QueryUtils.getBillingSummaries
import CsvUtils.{bqsToTrexRowList, writeToTrexCsv}

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
