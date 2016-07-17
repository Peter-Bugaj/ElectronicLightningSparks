// Library files.
import Sessionizer.SessionInfo
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

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
    runTests()

    // If all test passed, execute the sessionizer on the actual data.
    /**val input = this._sparkContext.textFile("data/realSample.log")
    Sessionizer.computeAverageSessionTime(input).
      saveAsTextFile("sessionsPerUser")
    Sessionizer.computeUniqueVisits(input).
      saveAsTextFile("uniqueVisits")*/
  }

  /**
    * Method for running test cases.
    */
  def runTests(): Unit = {
    //this.test_1_uniqueVisitPerUser()
    this.test_2_sessionsPerUser()
  }

  /**
    * Check that the unique visits per user are counted correctly.
    */
  def test_1_uniqueVisitPerUser(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/1_uniqueVisits")

    val results = Sessionizer.computeUniqueVisits(input).collect
    val uniqueVisitsPerUser = Array(
      ("123.242.248.130:54635", 2),
        ("1.39.32.179:56419", 3),
        ("59.183.41.47:62014", 2))

    results.foreach(p => {
      assert(uniqueVisitsPerUser.contains(p))
    })
  }

  /**
    * Check that the session are counted correctly for each user.
    */
  def test_2_sessionsPerUser(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/1_userSessions")

    val results = Sessionizer.computeAverageSessionTime(input)
    results.saveAsTextFile("test2")
    val uniqueVisitsPerUser = Array(
      ("23.242.248.130:54635", 2),
      ("123.242.248.130:54635", 2))

    results.foreach(p => {
      assert(uniqueVisitsPerUser.contains((p._1, p._2.count)))
    })
  }

  /**
    * Creates a spark context.
    */
  private def _createSparkContext: SparkContext = {
    val conf = new SparkConf().setAppName("Basic Spark Application")
    new SparkContext(conf)
  }
}
