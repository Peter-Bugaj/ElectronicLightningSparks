This is a challenge for PayTM that involves learning the basics of Spark. Project compiles and runs through SBT and can be added to IntelliJ for further support.

To execute app, run the following in command line:
1. $>: SBT

2. $>: compile

3. $>: package

4. $>: exit

5. $>: 

spark-submit \
   --class "SessionizeApp" \
   --master "local[*]" \
   target/scala-2.11/paytm-challenge_2.11-1.0.jar
