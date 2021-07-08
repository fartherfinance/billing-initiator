import java.time.{LocalDate, ZoneId}

import scala.concurrent.Await
import scala.concurrent.duration._
import BillingOps.QueryUtils._
import BillingOps.CsvUtils._
import BillingOps.{FARTHER_BILLING_ACCOUNT_ID, MONTHLY_RATE_NO_ADVISOR_BPS, MONTHLY_RATE_WITH_ADVISOR_BPS}
import org.scalatest.flatspec.AnyFlatSpec

class BillingQueryProcessingSpec extends AnyFlatSpec {

  "The billing query processing function " should "always return a non-empty result" in {
    val bqs = getBillingSummaries

    val bqsRes = Await.result(bqs, 5.seconds)
    assert(!bqsRes.isEmpty)
  }
}

class TrexBuildingSpec extends AnyFlatSpec {
  val te = 2856.32
  val expFeeWAdv = te * MONTHLY_RATE_WITH_ADVISOR_BPS
  val expFeeWoAdv = te * MONTHLY_RATE_NO_ADVISOR_BPS
  val dateString = "June-20"
  val accountCreatedOn = LocalDate
    .of(2020, 6, 1)
    .atStartOfDay(ZoneId.of("UTC"))
    .toInstant
  val bqsWAdvisor = BillingQuerySummary(
    clientId = 1,
    accountID = "8GU",
    totalEquity = te,
    advisorId = Some(1),
    accountCreated = "COMPLETE",
    accountType = "INDIVIDUAL",
    accountName = "Long term savings",
    createdOn = accountCreatedOn,
    clientFirstName = "Testy",
    clientLastName = "McTesterson",
    currentAccountBalance = 100.00
  )

  val bqsWoAdvisor = BillingQuerySummary(
    clientId = 1,
    accountID = "8GU",
    totalEquity = te,
    advisorId = None,
    accountCreated = "COMPLETE",
    accountType = "INDIVIDUAL",
    accountName = "Lake House",
    createdOn = accountCreatedOn,
    clientFirstName = "Testy",
    clientLastName = "McTesterson",
    currentAccountBalance = 100.00
  )

  "The billingQuerySummaryToTrexRow Function" should "produce a List[String] row ready for use in csv" in {

    val generatedTrexRow = billingQuerySummaryToTrexRow(bqsWAdvisor, dateString)


    assert(generatedTrexRow.isInstanceOf[List[String]])
  }

  it should "bill at the advisor rate when the client has selected an advisor" in {

    val expected = List(
      "1",
      "IA",
      "FEE",
      "8GU",
      expFeeWAdv.toString,
      "Farther Finance Monthly Management Fee",
      dateString,
      "IA",
      "FEE",
      FARTHER_BILLING_ACCOUNT_ID,
      "Farther Finance Monthly Management Fee",
      "Fee for #8GU"
    )

    val generated = billingQuerySummaryToTrexRow(bqsWAdvisor, dateString)

    assert(generated == expected)

  }

  it should "bill at the no advisor rate when the client has not selected an advisor" in {

    val expected = List(
      "1",
      "IA",
      "FEE",
      "8GU",
      expFeeWoAdv.toString,
      "Farther Finance Monthly Management Fee",
      dateString,
      "IA",
      "FEE",
      FARTHER_BILLING_ACCOUNT_ID,
      "Farther Finance Monthly Management Fee",
      "Fee for #8GU"
    )

    val generated = billingQuerySummaryToTrexRow(bqsWoAdvisor, dateString)

    assert(generated == expected)
  }

  "The bqsToTrexRowList function" should "create a list with all expected elements including header" in {

    val r1 = billingQuerySummaryToTrexRow(bqsWAdvisor, dateString)
    val r2 = billingQuerySummaryToTrexRow(bqsWoAdvisor, dateString)
    val expected = List(TrexHeaderRow, r1, r2)

    val inputSeq = Seq(bqsWAdvisor, bqsWoAdvisor)

    val generated = bqsToTrexRowList(inputSeq, dateString)

    assert(generated == expected)
  }

  "The writeToTrexCsv function" should "create a file (manually verify for now)" in {
    val fpath = "testtrexfile.csv"

    val r1 = billingQuerySummaryToTrexRow(bqsWAdvisor, dateString)
    val r2 = billingQuerySummaryToTrexRow(bqsWoAdvisor, dateString)
    val expected = List(TrexHeaderRow, r1, r2)

    val inputSeq = Seq(bqsWAdvisor, bqsWoAdvisor)

    val writeList = bqsToTrexRowList(inputSeq, dateString)

    writeToTrexCsv(writeList, fpath)

  }
}