package BillingOps

import java.io.{File, FileNotFoundException, IOException}
import java.time.{Instant, LocalDate, ZoneId, Period}

import scala.concurrent.ExecutionContext.Implicits.global
import slick.jdbc.MySQLProfile.api._
import com.github.tototoshi.csv._
import com.farther.northstardb._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object TestQuery extends App {
  // Get the Environment
  val envType = sys.env("ENV_TYPE")

  val dbConfPath = if (envType == "PROD") "ProdNsMysql" else "UatNsMysql"
  // Configure DB
  val db = Database.forConfig(dbConfPath)

  val fis = TableQuery[FartherInvestmentAccountsTable]

  val q1 = fis.take(5).result

  val qFut = db.run(q1)

  val res = Await.result(qFut, 5.seconds)

  println(res)

  val instants = res.map(_.createdOn)


}


/**
 * Object containing billing constants such as billing rates
 */

object BillingConstants {

  val MonthlyRateWithAdvisorBps = 0.008 / 12
  val MonthlyRateNoAdvisorBps = 0.004 / 12
  val FartherBillingAccountId = "8GU050031X"
  val minIRAAmountBilledinDollars = 2.0
  val minNonRetirementAccountBilled = 1.0
  // retirement accounts are roth and ira
}

object QueryUtils {
  import BillingConstants._

  // Types
  case class BillingQuerySummary(clientId: Int,
                                 accountID: String,
                                 totalEquity: Double,
                                 advisorId: Option[Int],
                                 accountType: String,
                                 accountName: String,
                                 createdOn: Instant,
                                 clientFirstName: String,
                                 clientLastName: String,
                                 currentAccountBalance: Double
                                )U

  // Get the Environment
  val envType = sys.env("ENV_TYPE")

  val dbConfPath = if (envType == "PROD") "ProdNsMysql" else "UatNsMysql"
  // Configure DB
  val db = Database.forConfig(dbConfPath)
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
  import BillingConstants._
  import BillingLogic.calcAccountBillingAmount

  // Header Row is constant
  val TrexHeaderRow = List("ClientId",
    "DrBatchB", "DrEntry", "DrAcct", "Amount", "DrTr1",
    "DrTr2", "CrBatchB", "CrEntry", "CrAcct", "CrTr1", "CrTr2",
    "FirstName", "LastName", "AcctBalance", "AdvisorID")

  /**
   * Returns a Trex Billing Row (List[String]) ready to write to csv
   * @param bqs - Billing Query Summary
   * @param monthYear - String in Month Abbreviation-Year (e.g. Jun-20)
   * @return trexRow List[String] - A List for writing to csv
   */
  def billingQuerySummaryToTrexRow(bqs: BillingQuerySummary, monthYear: String): List[String] = {

    val billingRate = bqs.advisorId match {
      case Some(_) => MonthlyRateWithAdvisorBps
      case None => MonthlyRateNoAdvisorBps
    }

    val billingAmount = calcAccountBillingAmount(billingRate, bqs.totalEquity,
      bqs.accountType, bqs.createdOn)

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
      FartherBillingAccountId,
      "Farther Finance Monthly Management Fee",
      s"Fee for #${bqs.accountID}",
      bqs.clientFirstName,
      bqs.clientLastName,
      bqs.currentAccountBalance.toString,
      bqs.advisorId.getOrElse("None").toString
    )

    trexRow
  }

  /**
   * Returns the list representation of a Trex file, ready for writing to csv
   * @param b Seq[BillingQuerySummary] - the returned result of a bill query
   * @param monthYear String - month year for the billing period e.g. Apr-20
   * @return trexRowList List[List[String]] - the list of all billing rows w/ header for writing to the csv
   */
  def bqsToTrexRowList(b: Seq[BillingQuerySummary], monthYear: String) = {
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
  import BillingConstants.{minIRAAmountBilledinDollars, minNonRetirementAccountBilled}
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
            (unadjustedFee > minIRAAmountBilledinDollars)
          ) ||
            (unadjustedFee > minNonRetirementAccountBilled)
          ) unadjustedFee
      else if ((accountType == "ira" || accountType == "roth") && (accountAUM > minIRAAmountBilledinDollars)) minIRAAmountBilledinDollars
      // TODO add joint account when the time comes
      else if (
        ((accountType == "emf") || (accountType == "lts") || (accountType == "lpa")) &&
          (accountAUM > minNonRetirementAccountBilled)) minNonRetirementAccountBilled
      else 0.0 // unrecognized account type
    }
    else 0.0
  }


  private def accountWasCreatedMoreThanOneMonthAgo(createdOnDate: Instant) = {
    val nowDate = Instant.now.atZone(ZoneId.of("UTC")).toLocalDate
    val createdOnLocalDate = createdOnDate.atZone(ZoneId.of("UTC")).toLocalDate
    val periodSinceCreation = Period.between(createdOnLocalDate, nowDate)
    val monthsSinceCreation = periodSinceCreation.getMonths
    val daysSinceCreation = periodSinceCreation.getDays

    if ((monthsSinceCreation >= 1) ||(daysSinceCreation >= 30)) true else false
  }

}