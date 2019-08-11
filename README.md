# rexxmqtt
Mainframe REXX MQTT implementation


Currently only sending is implemented.
Only TCPI (without TLS).

Call as
  
  sendmqtt mqtt://host<:port>/topic-path <feeding-method> <additional args>
 
Four metods of feeding data available:
  * SAFE - NetView SAFE (only available in NetView environment)
  * FILE - dataset is source of data (use TSO ALLOC, FREE and EXECIO)
  * STCK - rexx stack is source of data  
  * TEXT - last argument is data to send
 
  
