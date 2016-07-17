// Library files.
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by peterbugaj on 2016-07-17.
  */
object Sessionizer {

  /**
    * The time frame window to use for detecting new session (15 minutes).
    */
  private val _inactiveWindowMillis: Long = 900000l

  /**
    * Determine the session information per user.
    */
  def computeAverageSessionTime(input: RDD[String]): RDD[(String, SessionInfo, Double)] = {

    // Parse for the necessary information first
    // and sort by the time stamp.
    val requestsPerIp = input.map(line => {

      val lineArr = line.split(" ")
      val requestTime = this._computeStamp(lineArr(0))
      val userIp = lineArr(2)

      (userIp, this._createDefautlSessionInfo(requestTime))

    }).sortBy(p => p._2.stamp)

    // Compute the statistic for each session per user.
    val sessionsPerUser = requestsPerIp.foldByKey(
      this._createDefautlSessionInfo(-1)) (
      (acc, value) => this._updateSessionInfo(acc, value))

    // At the end compute the averages as well
    // and sort by longest session times to see
    // the most engaged users.
    val sessionResultsWithAverages = sessionsPerUser.map(nextPair => {
      val sessionInfo = nextPair._2

      val avg = sessionInfo.totalLength /
        sessionInfo.count

      (nextPair._1, sessionInfo, avg)
    }).sortBy(a=> a._2.longest, ascending = false)


    // Write the output.
    sessionResultsWithAverages
  }

  /**
    * Determine the unique IP visit per user.
    */
  def computeUniqueVisits(input: RDD[String]): RDD[(String, Int)] = {

    // Parse for the necessary information first.
    val infoPerUser = input.map(line => {
      val lineArr = line.split(" ")
      val userIp = lineArr(2)
      val requestAddr = lineArr(3)

      ((userIp, requestAddr), 1)
    })

    // Get the unique vists per user
    val uniqueVistPerUser = infoPerUser.
      reduceByKey(_ + _).
      map(p => (p._1._1, p._2)).
      reduceByKey(_ + _)

    // Write the output.
    uniqueVistPerUser
  }

  /**
    * Helper function to update session information
    * as part of an accumulative function.
    */
  private def _updateSessionInfo(
    acc: SessionInfo,
    value: SessionInfo): SessionInfo = {

    // Determine if there is a new user session.
    val isNewSession = this._isNewSession(acc.stamp, value.stamp)
    if (isNewSession) {

      // If there is a new session, increase the session count, reset
      // the current length to zero, and keep the total session length
      // the same, as the inactive time (which was more than 15 minutes)
      // does not count as part of session activity.
      SessionInfo(
        stamp = value.stamp,
        count = acc.count + 1,
        totalLength = acc.totalLength,
        currentLength = 0,
        longest = acc.longest)

    } else {

      // Otherwise an existing session is still taking place. In this
      // case update the current session length, update the total
      // session activity, and keep track of the longest session seen so far.
      val timeSinceLastRequest = value.stamp - acc.stamp
      val newCurrentLength = acc.currentLength + timeSinceLastRequest
      val newTotalLength = acc.totalLength + timeSinceLastRequest

      SessionInfo(
        stamp = value.stamp,
        count = acc.count,
        totalLength = newTotalLength,
        currentLength = newCurrentLength,
        longest = Math.max(newCurrentLength, acc.longest))
    }
  }

  /**
    * Helper function to create default session info.
    */
  private def _createDefautlSessionInfo(stamp: Long): SessionInfo = {
    SessionInfo(stamp = stamp, count = 0, totalLength = 0, currentLength = 0, longest = 0)
  }

  /**
    * Help compute the time stamp in milliseconds.
    */
  private def _computeStamp(input: String): Long = {
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").
      parse(input.substring(0, 23) + "Z").getTime
  }

  /**
    * Determine if a new session started based on
    * the previous and current time stamp.
    */
  private def _isNewSession(prev: Long, curr: Long): Boolean = {
    curr < 0 || prev < 0 || (curr - prev) >= this._inactiveWindowMillis
  }

  /**
    * Class for helping to count sessions.
    */
  case class SessionInfo (
    stamp: Long,
    count: Int,
    totalLength: Double,
    currentLength: Double,
    longest: Double)
}
