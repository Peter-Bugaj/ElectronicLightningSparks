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




 timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol


 additional info; whether user was idle ont the webpage and starring at the screen especially if 
 reading a document. Detect mouse moves and scroll events

 challenge:
 first I tried getting the session per user, finding length of each session, and then computing the average
 But then I realized I could do this using jsut the aggregate function and increase counter based on interval
 then I realized i could also compute average in the aggregate

 Decided to use diagram to see what mapping would do. From there I figured out I could use aggregate.