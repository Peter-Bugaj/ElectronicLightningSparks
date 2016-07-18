##Description##
This is a challenge for PayTM that involves learning the basics of Spark. Project compiles and runs through SBT and can be added to IntelliJ for further support.

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
The following test cases where done:

###Check that the unique URL visits per user are counted correctly.###
test_1_uniqueVisitPerUser

###Check that the session are counted correctly for one user.###
test_2_sessionsPerOneUser