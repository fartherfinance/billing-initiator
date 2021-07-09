package BillingOps

import java.io.{File, FileNotFoundException, IOException}
import java.time.{Instant, Period, ZoneId}
import java.util
import java.util.UUID

import BillingOps.BillingLogic.generateAndExecuteFeeBatch
import BillingOps.CsvUtils.{bqsToTrexRowList, writeToTrexCsv}
import BillingOps.QueryUtils.{BillingQuerySummary, getBillingSummaries}
import com.farther.grecoevents.FeeEvents.{ExecuteFeeBatch, ExecuteFeeBatchData, ExecuteFeeData}
import com.farther.grecoevents.{EventSource, InitiatingParty}
import slick.jdbc.MySQLProfile.api._
import com.github.tototoshi.csv._
import com.farther.northstardb._
import com.farther.pubsub.kafka.ConfiguredKafkaProducer
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, RecordMetadata}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


/**
 * Object containing billing constants such as billing rates
 */

object QueryUtils {
  // Types
  case class BillingQuerySummary(clientId: Int,
                                 accountID: String,
                                 totalEquity: Double,
                                 advisorId: Option[Int],
                                 accountCreated: String,
                                 accountType: String,
                                 accountName: String,
                                 createdOn: Instant,
                                 clientFirstName: String,
                                 clientLastName: String,
                                 currentAccountBalance: Double
                                )

  // Configure DB
  val db = Database.forConfig(DB_CONF_PATH)
  // Get accounts to be billed by user and assign rate / billing amount to clients

  /**
   * Returns a Future[Seq[BillingSummary]]  for use in billing operations
   *
   * Queries the DB for lists of clients who are eligible for billing
   * We use the results to determine the eligibility
   *
   * Selection Filter for Eligibility:
   * 1. A client has a "real" account (meaning the initiation process is complete)
   *  - farther_investment_accounts.account_created = "COMPLETE"
   *
   * Required Information
   *
   * 1. Selected Advisor ID - A client's tier is determined by whether they have an advisor
   *  - investment_details.advisor_id is not null
   */
  def getBillingSummaries: Future[Seq[BillingQuerySummary]] = {

    // Establish our table queries
    val fas = TableQuery[FartherAccountSummaryTable]
    val invDets = TableQuery[InvestmentDetailsTable]
    val investmentAccountDets = TableQuery[FartherInvestmentAccountsTable]
    val userDets = TableQuery[UserDetailsTable]

    val fasInvDetJoin = for {
      accountSummary <- fas.filter(s => (s.status === "CREATED") || (s.status === "1"))
      investmentDetails <- invDets if investmentDetails.idUser === accountSummary.idUser
      invAccountDetails <- investmentAccountDets if invAccountDetails.id === accountSummary.accountId
      coreUserDetails <- userDets if coreUserDetails.idUser === accountSummary.idUser
    } yield (accountSummary.idUser,
      accountSummary.accountId,
      accountSummary.totalEquity,
      investmentDetails.advisorId,
      invAccountDetails.accountCreated,
      accountSummary.accountType,
      accountSummary.accountName,
      invAccountDetails.createdOn,
      coreUserDetails.firstName.getOrElse(""),
      coreUserDetails.lastName.getOrElse(""),
      accountSummary.totalEquity)
      .mapTo[BillingQuerySummary]


    val fasInvDetJoinAction = fasInvDetJoin.result

    val fasInvDetJoinFut = db.run(fasInvDetJoinAction)

    fasInvDetJoinFut

  }
}


/**
 * Functions for preparing and writing billing information to CSV
 */
object CsvUtils {
  import QueryUtils.BillingQuerySummary
  import BillingLogic.calcAccountBillingAmount

  // Header Row is constant
  val TrexHeaderRow = List("ClientId",
    "DrBatchB", "DrEntry", "DrAcct", "Amount", "DrTr1",
    "DrTr2", "CrBatchB", "CrEntry", "CrAcct", "CrTr1", "CrTr2",
    "FirstName", "LastName", "AcctBalance")

  /**
   * Returns a Trex Billing Row (List[String]) ready to write to csv
   * @param bqs - Billing Query Summary
   * @param monthYear - String in Month Abbreviation-Year (e.g. Jun-20)
   * @return trexRow List[String] - A List for writing to csv
   */
  def billingQuerySummaryToTrexRow(bqs: BillingQuerySummary, monthYear: String): List[String] = {

    val billingRate = bqs.advisorId match {
      case Some(_) => MONTHLY_RATE_WITH_ADVISOR_BPS
      case None => MONTHLY_RATE_NO_ADVISOR_BPS
    }

    val billingAmount = calcAccountBillingAmount(billingRate, bqs.totalEquity, bqs.accountType, bqs.createdOn)

    val trexRow = List(
      bqs.clientId.toString,
      "IA",
      "FEE",
      bqs.accountID + "X", // X needed for cash account
      billingAmount.toString,
      "Farther Finance Monthly Management Fee",
      monthYear,
      "IA",
      "FEE",
      FARTHER_BILLING_ACCOUNT_ID,
      "Farther Finance Monthly Management Fee",
      s"Fee for #${bqs.accountID}",
      bqs.clientFirstName,
      bqs.clientLastName,
      bqs.currentAccountBalance.toString
    )

    trexRow
  }

  /**
   * Returns the list representation of a Trex file, ready for writing to csv
   * @param b Seq[BillingQuerySummary] - the returned result of a bill query
   * @param monthYear String - month year for the billing period e.g. Apr-20
   * @return trexRowList List[List[String]] - the list of all billing rows w/ header for writing to the csv
   */
  def bqsToTrexRowList(b: Seq[BillingQuerySummary], monthYear: String): List[List[String]] = {
    val bqsRowSeq = b map {bqs => billingQuerySummaryToTrexRow(bqs, monthYear)}
    val bqsRowList = bqsRowSeq.toList
    val trexRowList = TrexHeaderRow :: bqsRowList

    trexRowList
  }

  /**
   * Writes a trexRowList to Csv
   * @param c - List[List[String]] - The prepared list for output to trex
   * @param fpath - String - The path to where the file should be written
   */
  def writeToTrexCsv(c: List[List[String]], fpath: String): Unit = {
    val outFile = new File(fpath)

    val writer = CSVWriter.open(outFile)
    try {
      writer.writeAll(c)
    } catch {
      case e: FileNotFoundException => println("File not found exception")
      case e: IOException => println(s"IO Exception ${e}")
    } finally {
      writer.close()
    }
  }

}

object BillingLogic {
  /**
   * Returns (on a per account basis) the amount (in dollars) to bill to the account
   *
   * @param feeRate percentage rate of AUM fees for the client
   * @param accountAUM amount of assets in USD in the target account
   * @param accountType account type (INDIVIDUAL, ROTH_IRA, TRADITIONAL_IRA)
   * @return billingAmount the amount for the account
   */
  def calcAccountBillingAmount(feeRate: Double, accountAUM: Double,
                               accountType: String, createdOnDate: Instant): Double = {
    if (accountWasCreatedMoreThanOneMonthAgo(createdOnDate))
    {

      val unadjustedFee = feeRate * accountAUM

      if (
          (
            (accountType == "ira" || accountType == "roth") &&
            (unadjustedFee > MIN_IRA_AMOUNT_BILLED_DOLLARS)
          ) ||
            (unadjustedFee > MIN_NON_IRA_AMOUNT_BILLED_DOLLARS)
          ) unadjustedFee
      else if ((accountType == "ira" || accountType == "roth") && (accountAUM > MIN_IRA_AMOUNT_BILLED_DOLLARS)) MIN_IRA_AMOUNT_BILLED_DOLLARS
      // TODO add joint account when the time comes
      else if (
        ((accountType == "emf") || (accountType == "lts") || (accountType == "lpa")) &&
          (accountAUM > MIN_NON_IRA_AMOUNT_BILLED_DOLLARS)) MIN_NON_IRA_AMOUNT_BILLED_DOLLARS
      else 0.0 // unrecognized account type
    }
    else 0.0
  }


  private def accountWasCreatedMoreThanOneMonthAgo(createdOnDate: Instant): Boolean = {
    val nowDate = Instant.now.atZone(ZoneId.of("UTC")).toLocalDate
    val createdOnLocalDate = createdOnDate.atZone(ZoneId.of("UTC")).toLocalDate
    val periodSinceCreation = Period.between(createdOnLocalDate, nowDate)
    val monthsSinceCreation = periodSinceCreation.getMonths
    val daysSinceCreation = periodSinceCreation.getDays

    if ((monthsSinceCreation >= 1) ||(daysSinceCreation >= 30)) true else false
  }


  def generateFee(bqs: BillingQuerySummary): ExecuteFeeData = {
    val billingRate = bqs.advisorId match {
      case Some(_) => MONTHLY_RATE_WITH_ADVISOR_BPS
      case None => MONTHLY_RATE_NO_ADVISOR_BPS
    }

    ExecuteFeeData(amount = calcAccountBillingAmount(billingRate, bqs.totalEquity, bqs.accountType, bqs.createdOn),
                   account = bqs.accountID,
                   contraAccount = FARTHER_BILLING_ACCOUNT_ID_FORMATTED,
                   accountSideDescription = List("Monthly Management Fee"),
                   contraAccountSideDescription = List(s"Fee for #${bqs.accountID}"),
                   feeType = MANAGEMENT_FEE_TYPE)
  }

  def generateFeeBatch(bqs: List[BillingQuerySummary]): ExecuteFeeBatch = {
    val fees = bqs.map(generateFee).filter(_.amount > 0)

    ExecuteFeeBatch(correlationId = UUID.randomUUID().toString,
                    inServiceOfClientId = -1,
                    eventData = ExecuteFeeBatchData(fees),
                    eventSource = EventSource("", ""),
                    initiatingParty = InitiatingParty("", ""),
                    timeSent = Instant.now().toString)
  }

  def generateAndExecuteFeeBatch(bqs: List[BillingQuerySummary], topic: String)
                                (implicit kafkaProducer: KafkaProducer[String, String],
                                 producerFuncs: KafkaProducerFuncs): util.concurrent.Future[RecordMetadata] = {

    val executeFeeBatch = generateFeeBatch(bqs)

    KafkaProducerFuncs.publishToKafka(executeFeeBatch, topic)
  }

}

object ExecutionUtils {

  /**
   * Generates file with each accounts' amount to be billed
   * @param monthYear should be of format: Mmm-YY (Jun-21)
   */
  def generateBillingFile(monthYear: String): Unit = {
    // Load Billing Configs
    val billingConf = ConfigFactory
      .load("application.conf")
      .getConfig("Billing")
    val basePath = billingConf.getString("outFileBasePath")
    println(s"File base path is $basePath")

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


  /**
   * Generates event for initaiting a batch of fees for given month's monthly funding
   */
  def executeBilling(): Unit = {
    implicit val kafkaProducer: KafkaProducer[String, String] = new ConfiguredKafkaProducer().producer
    implicit val producerFuncs: KafkaProducerFuncs = new KafkaProducerFuncs()

    val bqsFut = getBillingSummaries
    val bqs = Await.result(bqsFut, 60.seconds).toList

    generateAndExecuteFeeBatch(bqs, KAFKA_TEST_TOPIC)
    Thread.sleep(5)

    println("Operation Complete")
  }
}