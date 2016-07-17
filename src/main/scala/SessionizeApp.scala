import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by peterbugaj on 2016-07-15.
  */
object SessionizeApp {

  private val _sparkContext = this._createSparkContext

  /**
    * Main function.
    */
  def main(args: Array[String]) {

    // Run test cases first before analyzig the actual data.
    val testFailed = runTests

    // If all test passed, execute on the actual data.
    if(testFailed == 0) {

      val input = this._sparkContext.
        textFile("data/2015_07_22_mktplace_shop_web_log_sample.log")
      Sessionizer.computeAverageSessionTime(input).
        saveAsTextFile("sessionsPerUser")
      Sessionizer.computeUniqueVisits(input).
        saveAsTextFile("uniqueVisits")
    }
  }

  /**
    * Method for running test cases.
    */
  def runTests: Int = {
    return 0
  }

  /**
    * Creates a spark context.
    */
  private def _createSparkContext: SparkContext = {
    val conf = new SparkConf().setAppName("Basic Spark Application")
    new SparkContext(conf)
  }
}
