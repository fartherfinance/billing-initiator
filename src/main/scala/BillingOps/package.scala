package object BillingOps {

  val MONTHLY_RATE_WITH_ADVISOR_BPS: Double = 0.008 / 12
  val MONTHLY_RATE_NO_ADVISOR_BPS: Double = 0.004 / 12
  val FARTHER_BILLING_ACCOUNT_ID = "8GU050031X"
  val FARTHER_BILLING_ACCOUNT_ID_FORMATTED = "8GU050031"
  val MIN_IRA_AMOUNT_BILLED_DOLLARS = 2.0
  val MIN_NON_IRA_AMOUNT_BILLED_DOLLARS = 1.0
  // retirement accounts are roth and ira

  val ENV_TYPE: String = sys.env("ENV_TYPE")

  val DB_CONF_PATH: String = if (ENV_TYPE == "PROD") "ProdNsMysql" else "UatNsMysql"

  val MANAGEMENT_FEE_TYPE = "MANAGEMENT_FEE"

  val KAFKA_TEST_TOPIC = "uatTestTopic"
  val KAFKA_TRANSFERS_TOPIC = "greco.transfers.ach"

}
