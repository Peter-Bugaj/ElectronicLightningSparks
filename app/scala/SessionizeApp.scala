// Library files.
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
  * Created by peterbugaj on 2016-07-15.
  */
object SessionizeApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Basic Spark Application")
    val sc = new SparkContext(conf)

    val fileName = args(0)
    val lines = sc.textFile(fileName).cache

    val c = lines.count
    println(s"There are $c lines in $fileName")
  }
}
