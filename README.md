##Description##
Spark project that compiles and runs through SBT and which can be added to IntelliJ for further support.

##Author##
Peter Bugaj

##Running the code##
To execute the app, run the following in command line within the project directory:
1. $>: SBT

2. $>: compile

3. $>: package

4. $>: exit

5. $>: spark-shell

7. $>: cd [PATH TO PROJECT DIRECTORY]

8. $>: spark-submit \
   --class "SessionizeApp" \
   --master "local[*]" \
   target/scala-2.11/paytm-challenge_2.11-1.0.jar

##Testing##
The following test cases where done **(with test code found in ./src/main/scala/SessionizeApp and with test data files found in ./tests)**:

###Check that the unique URL visits per user are counted correctly###
test_1_uniqueVisitPerUser

-
###Check that the session are counted correctly for one user###
test_2_sessionsPerOneUser

-
###Check that the session are counted correctly for multiple users###
test_3_sessionsPerMultipleUsers

-
###Check that the session are of correct length in the case wher the user visited the site only once during a session. In other words the sessions are recorded as having length zero.###
test_4_sessionsWithZeroLength

-
###Check that the session are of correct length for each user IP###
test_5_sessionsWithLength

-
###Check that the session averages are correctly computed per user IP###
test_6_sessionsAverages

-
###Check that the session averages are correctly computed, where some of the sessions are of length zero###
test_7_sessionsAveragesWithZeroLength

-
###Find the longest session for one specific user IP address###
test_8_longestSessionOneUser

-
###Find the longest session for multiple different users IP addresses, with each user having multiple sessions###
test_9_longestSessionMultipleUsers

-
###Ensure that the data is returned, sorted by users with the longest session length, so to be able to see the most engaged users on top###
test_10_getMostEngagedUsers

-
###Ensure that everything still works and that the longest session lengths are returned, even when the input data is not sorted by request times###
test_11_useUnsortedData
