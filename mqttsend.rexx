/* rexx */                                                                      
/*                                                                              
See http://www.ibm.com/developerworks/webservices/library/ws-mqtt/index.html    
    for the full protocol spec                                                  
                                                                                
 (C) 2011 Dougie Lawson, all rights reserved - version for Non-z/OS             
 (C) 2019 Piotr Kolasinski, all rights reserved - version for z/OS              
*/                                                                              
/*                                                                              
  Parameters:                                                                   
  arg1 - mqtt uri: mqtt://HOST:<PORT>/TOPIC                                     
  arg2 - transport method (channel) for data:                                   
         SAFE - NetView SAFE                                                    
         FILE - data read from file                                             
         STCK - data read from stack                                            
         TEXT - date get from 3rd argument                                      
  arg3 - addtional info for channel                                             
         name of SAFE                                                           
         file name (DD or DSN)                                                  
         delimiter character f.ex. X'00'                                        
         text to send                                                           
*/                                                                              
                                                                                
dbg = 0                                                                         
                                                                                
if dbg \= 0 then                                                                
    say "MQTTSEND debug enabled!"                                               
                                                                                
parse arg mqttserv channel srcname                                              
                                                                                
parse var mqttserv "mqtt://" dest "/" topic                                     
parse var dest host ":" port                                                    
                                                                                
if port = '' then port = 1883                                                   
if host = '' then host = 'localhost'                                            
if topic = '' then                                                              
  topic = '/mainframe/test'                                                     
else                                                                            
  topic = "/"topic                                                              
                                                                                
keepalive = 1 /* second */                                                      
                                                                                
/* init conversion */                                                           
                                                                                
call conv_ini                                                                   
                                                                                
call dbgmsg "***MQTTSEND executed***"                                           
                                                                                
if  symbol('CHANNEL') = 'LIT' then                                              
do                                                                              
  say "No channel defined, exiting !"                                           
  exit 8                                                                        
end                                                                             
select                                                                          
  when channel = "TEXT" then                                                    
    content = srcname                                                           
  when channel = "SAFE" then                                                    
    content = channel_safe(srcname)                                             
  when channel = "STCK" then                                                    
    content = channel_stck(srcname)                                             
  when channel = "FILE" then                                                    
    content = channel_file(srcname)                                             
    say content                                                                 
  otherwise                                                                     
    say "No channel method defined, exiting !"                                  
    say "Use: <mqtt-uri> <transport-method> <addtional-pamrs>"                  
    say "Available transports: TEXT,SAFE,STCK,FILE"                             
    exit 8                                                                      
end                                                                             
                                                                                
id = 'NETVMQTT pub client'                                                      
qos = 1 /* QOS must be set to 0 if WillFlag is set to 0 */                      
                                                                                
stat = socket("Initialize", "NETVMQTT")                                         
call check_stat "Initialize", stat                                              
                                                                                
rc = mqtt_connect(host, port, id, keepalive, 0)                                 
if rc <> 0 then do                                                              
     say "connect failed:" rc                                                   
     sock = socket("CLOSE", s);                                                 
     exit 1                                                                     
end                                                                             
                                                                                
rc = mqtt_publish(topic, content, 1)                                            
                                                                                
rc = mqtt_disconnect()                                                          
                                                                                
sock = socket("Close", s);                                                      
sock = socket("TERMINATE", "NETVMQTT")                                          
exit 0                                                                          
                                                                                
/* *********************************************** */                           
/*                                                 */                           
/*                                                 */                           
/*                                                 */                           
/* *********************************************** */                           
mqtt_disconnect:                                                                
                                                                                
   discode = 'E0'x                                                              
                                                                                
   /*     DISCONNECT 00            */                                           
   msg = 'E000'x                                                                
                                                                                
   call diag '---->', msg                                                       
                                                                                
   stat = socket("SEND", s, msg)                                                
   call check_stat "SEND", stat                                                 
   parse var stat rc .                                                          
                                                                                
   return rc                                                                    
                                                                                
/* *********************************************** */                           
/*                                                 */                           
/*                                                 */                           
/*                                                 */                           
/* *********************************************** */                           
mqtt_publish:                                                                   
   topic = conv_e2a(arg(1))                                                     
   content = conv_e2a(arg(2))                                                   
   qos_p  = arg(3)                                                              
                                                                                
   msgid = random()                                                             
   msgid = x2c(d2x(msgid,4))                                                    
   /* shift qos_p 1 bit left before conersion to char */                        
   qos1 = x2c(d2x(2 * qos_p, 2))                                                
   qos1  = bitand('06'x, qos1)  /* only two bits important */                   
                                                                                
   lth = hexlth(topic,4)                                                        
   if qos_p < 1 then                                                            
     buf = lth || topic || content                                              
   else                                                                         
     buf = lth || topic || msgid || content                                     
                                                                                
   hlth = mqtt_length_encode(length(buf))                                       
                                                                                
   pubcode = bitor('30'x, qos1)                                                 
                                                                                
   /*     PUBLISH      encoded len */                                           
   header = pubcode || hlth                                                     
   data = header || buf                                                         
                                                                                
   call diag '---->', data                                                      
   if qos_p > 0 then                                                            
     call diag 'Msgid:', c2x(msgid)                                             
                                                                                
   stat = socket("SEND", s, data)                                               
   call check_stat "SEND", stat                                                 
   parse var stat . len rsp                                                     
                                                                                
   if qos_p == 2 then     /* qos_p == 2 - exactly once delivery */              
   do                                                                           
      call diag "QOS=2 - wait for PUBREC"                                       
      stat = socket("RECV", s, 4) /* get PUBREC package */                      
      call check_stat "RECV", stat                                              
      parse var stat . size rsp1                                                
                                                                                
      call diag '<----', rsp1                                                   
                                                                                
      if length(rsp1) > 0 then                                                  
      do                                                                        
         parse var rsp1 1 verb 2 rlth 3 rmsgid                                  
                                                                                
         call diag 'Rmsgid:', c2x(rmsgid)                                       
         call diag 'Msgid:',  c2x(msgid)                                        
                                                                                
         if  c2d(msgid) = c2d(rmsgid) then                                      
         do                                                                     
           /* SEND PUBREL */                                                    
           msg = '6202'x || rmsgid                                              
                                                                                
           call diag '---->', msg                                               
                                                                                
           stat = socket("SEND", s, msg)                                        
           call check_stat "SEND", stat                                         
           parse var stat . len rsp                                             
                                                                                
           /* Get PUBCOMP */                                                    
           stat = socket("RECV", s, 5)                                          
           call check_stat "RECV", stat                                         
           parse var stat . len rsp2                                            
                                                                                
           call diag '<----', rsp2                                              
         end                                                                    
         else                                                                   
           say "Different message ids"                                          
       end                                                                      
   end                                                                          
   else if qos_p == 1 then do  /* qos_p == 1 - at least one delivere */         
      call diag "QOS=1 - wait for PUBACK"                                       
      stat = socket("RECV", s, 4) /* get PUBACK package */                      
      call check_stat "RECV", stat                                              
      parse var stat . size puback                                              
                                                                                
      call diag '<----', puback                                                 
                                                                                
      end                                                                       
   else do /* qos_p == 0 - at most one delivery - no packages suspected */      
      call diag "QOS=0 - no other packages"                                     
   end                                                                          
                                                                                
   return 0                                                                     
                                                                                
/* *********************************************** */                           
/*                                                 */                           
/*                                                 */                           
/*                                                 */                           
/* *********************************************** */                           
mqtt_connect:                                                                   
                                                                                
/* set up the MQTT CONNECT  and check the CONNACK */                            
                                                                                
   host = arg(1)                                                                
   port = arg(2)                                                                
   id = arg(3)                                                                  
   keepalive = arg(4)                                                           
   qos = arg(5)                                                                 
                                                                                
   qos = qos * 2  /* bit shift left one position */                             
   qos = x2c(d2x(qos,2))                                                        
                                                                                
   s = tcp_connect(host, port)                                                  
                                                                                
   ka = x2c(d2x(keepalive,4))                                                   
                                                                                
   /* calc len of client id */                                                  
   lth = hexlth(id,4)                                                           
                                                                                
   clientid = lth || conv_e2a(id)                                               
                                                                                
   /* Set up connect struct. */                                                 
   /*     ll        protocol   version   conflags   */                          
   /*                                    clean      */                          
                                                                                
   buf = '0004'x || conv_e2a('MQTT') || '04'x || '02'x ||  ka || clientid       
                                                                                
   /* header length */                                                          
   hlth = mqtt_length_encode(length(buf))                                       
                                                                                
   /*      CONNECT   1-byte total len */                                        
   concode = bitor('10'x, qos)                                                  
   header = concode || hlth                                                     
                                                                                
   msg = header || buf                                                          
                                                                                
   call diag '---->', msg                                                       
                                                                                
   stat = socket("SEND", s, msg)                                                
   call check_stat "SEND", stat                                                 
   parse var stat . size                                                        
                                                                                
   stat = socket("RECV", s, 5, 'MSG_PEEK')                                      
   call check_stat "RECV", stat                                                 
   parse var stat . len rsp                                                     
                                                                                
   call diag '<----', rsp                                                       
                                                                                
   parse var rsp 1 verb 2 rlth                                                  
                                                                                
   lth = mqtt_length_decode(rlth)                                               
   parse var lth rlth offset                                                    
   rlth = rlth + 2                                                              
                                                                                
   if rlth > 0 then do                                                          
       stat = socket("RECV", s, rlth)                                           
       call check_stat "RECV", stat                                             
       parse var stat . len rsp                                                 
   end                                                                          
                                                                                
   call diag '<----', rsp                                                       
                                                                                
   parse var rsp 1 verb 2 lth =(offset) rsn                                     
   rsn = right(rsn,1)                                                           
                                                                                
   if c2x(rsn) <> '00' then do                                                  
      say "verb:" c2x(verb)                                                     
      say "lth:"  c2x(lth)                                                      
      say "rsn:"  c2x(rsn)                                                      
      return -4096                                                              
   end                                                                          
   else return 0                                                                
                                                                                
/* *********************************************** */                           
/*                                                 */                           
/*                                                 */                           
/*                                                 */                           
/* *********************************************** */                           
tcp_connect: procedure expose dbg                                               
   host = arg(1)                                                                
   port = arg(2)                                                                
   /*                                                                           
   stat = socket('GetHostByName', host)                                         
   call check_stat "GetHostByName", stat                                        
   */                                                                           
   /* Get a stream socket. */                                                   
   stat = socket('Socket', 'AF_INET', 'SOCK_STREAM', 0)                         
   call check_stat 'Socket', stat                                               
   parse var stat . sockid                                                      
                                                                                
   /* Connect to the server. */                                                 
                                                                                
   stat = socket('Connect', sockid, '2 'port' 'host)                            
   call check_stat "Connect", stat                                              
                                                                                
   /* Set NO_DELAY socket. */                                                   
                                                                                
   stat = socket('SetSockOpt', sockid, 'IPPROTO_TCP', 'TCP_NODELAY', 0)         
   call check_stat 'SetSockOpt', stat                                           
                                                                                
   stat = socket('SetSockOpt', sockid, 'SOL_SOCKET', 'SO_SNDBUF', 8)            
   call check_stat 'SetSockOpt', stat                                           
                                                                                
   return sockid                                                                
                                                                                
/* *********************************************** */                           
/* Check status of socket function and exit if     */                           
/* error (rc <> 0)                                 */                           
/*                                                 */                           
/* *********************************************** */                           
check_stat:                                                                     
   parse arg func, stat                                                         
   if dbg \= 0 then                                                             
   do                                                                           
       say "FUNC:" func "STAT:" stat                                            
   end                                                                          
   parse var stat rc err msg                                                    
   if rc <> 0 then do                                                           
     say func "failed:" rc "<"msg">"                                            
     result = socket("Terminate", "NETVMQTT")                                   
     exit rc                                                                    
   end                                                                          
   return                                                                       
                                                                                
/* *********************************************** */                           
/*                                                 */                           
/*                                                 */                           
/*                                                 */                           
/* *********************************************** */                           
error:                                                                          
   result = socket("Terminate", "NETVMQTT")                                     
   call check_stat 'Terminate', stat                                            
                                                                                
   return                                                                       
                                                                                
/* *********************************************** */                           
/*                                                 */                           
/*                                                 */                           
/*                                                 */                           
/* *********************************************** */                           
hexlth: procedure                                                               
   id = arg(1)                                                                  
   rlth = arg(2)                                                                
   return x2c(d2x(length(id), rlth))                                            
                                                                                
/* *********************************************** */                           
/*  Calculate runlen mqtt format from real length  */                           
/* *********************************************** */                           
mqtt_length_encode: procedure                                                   
                                                                                
   x = arg(1)                                                                   
   remlen = ""                                                                  
   do while(x > 0)                                                              
                                                                                
      digit = x // 128                                                          
      x = x % 128                                                               
                                                                                
      if x > 0 then digit = digit + 128                                         
      remlen = remlen || d2x(digit,2)                                           
                                                                                
   end                                                                          
                                                                                
   return x2c(remlen)                                                           
                                                                                
/* *********************************************** */                           
/*  Calculate real length from runlen mqtt format  */                           
/* *********************************************** */                           
mqtt_length_decode: procedure                                                   
                                                                                
   remlen = arg(1)                                                              
   digits = c2x(remlen)                                                         
                                                                                
   multiplier = 1                                                               
   value = 0                                                                    
   done = 0                                                                     
   offset = 3                                                                   
   do i = 1 to length(digits) by 2                                              
                                                                                
      digit = x2d(substr(digits,i,2))                                           
      if digit > 128 then digit = digit - 128                                   
      else done = 1                                                             
      value = value + (digit * multiplier)                                      
      if done = 1 then leave                                                    
      multiplier = multiplier * 128                                             
      offset = offset + 1                                                       
   end                                                                          
   ret = value offset                                                           
   return ret                                                                   
                                                                                
/* ******************************************** */                              
/* Generate diagnostic messages                 */                              
/* ******************************************** */                              
diag: procedure expose dbg                                                      
  if dbg <> 0 then do                                                           
    parse arg hdr, msg                                                          
    say hdr                                                                     
    say msg                                                                     
    say c2x(msg)                                                                
  end                                                                           
                                                                                
   return                                                                       
                                                                                
dbgmsg:  procedure expose dbg                                                   
  if dbg \= 0 then do                                                           
    parse arg message                                                           
    /*                                                                          
    'PIPE var message | QSAM PSYPLOG APPEND'                                    
    */                                                                          
    say message                                                                 
  end                                                                           
  return                                                                        
                                                                                
/* ******************************************** */                              
/* Init global tables for simple conversion     */                              
/* ******************************************** */                              
                                                                                
conv_ini: procedure expose conv_a conv_e                                        
  e0 = '0123456789'                                                             
  a0 = xrange('30'X, '39'X)                                                     
  e1 = 'abcdefghi'                                                              
  a1 = xrange('61'X, '69'X)                                                     
  e2 = 'jklmnopqr'                                                              
  a2 = xrange('6A'X, '72'X)                                                     
  e3 = 'stuvwxyz'                                                               
  a3 = xrange('73'X, '7A'X)                                                     
  e4 = '@ABCDEFGHI'                                                             
  a4 = xrange('40'X, '49'X)                                                     
  e5 = 'JKLMNOPQR'                                                              
  a5 = xrange('4A'X, '52'X)                                                     
  e6 = 'STUVWXYZ'                                                               
  a6 = xrange('53'X, '5A'X)                                                     
  e7 = ' !"#$%&' || "'" || '()*+,-./'                                           
  a7 = xrange('20'X, '2F'X)                                                     
  e8 = ':;<=>?'                                                                 
  a8 = xrange('3A'X, '3F'X)                                                     
  e9 = '[\]^_'                                                                  
  a9 = xrange('5B'X, '5F'X)                                                     
  ea = '{|}~'                                                                   
  aa = xrange('7B'X, '7E'X)                                                     
                                                                                
  conv_a = a0 || a1 || a2 || a3 || a4 || a5 || a6 || a7 || a8 || a9 || aa       
  conv_e = e0 || e1 || e2 || e3 || e4 || e5 || e6 || e7 || e8 || e9 || ea       
                                                                                
  return                                                                        
                                                                                
/* ******************************************** */                              
/*  Simple conversion ascii --> ebcdic          */                              
/* ******************************************** */                              
                                                                                
conv_a2e: procedure expose conv_a conv_e                                        
  parse arg s                                                                   
  return translate(s, conv_e, conv_a)                                           
                                                                                
/* ******************************************** */                              
/*  Simple conversion ebcdic --> ascii          */                              
/* ******************************************** */                              
                                                                                
conv_e2a: procedure expose conv_a conv_e                                        
  parse arg s                                                                   
  return translate(s, conv_a, conv_e)                                           
                                                                                
                                                                                
/* ******************************************** */                              
/*  Reading from NetView SAFE source            */                              
/* ******************************************** */                              
channel_safe:                                                                   
  parse arg safename                                                            
  if safename = 'LIT' then safename = '*'                                       
  'pipe safe 'safename' | count lines | var linecnt'                            
                                                                                
  /* Get subject */                                                             
  'pipe safe 'safename' | var subj '                                            
                                                                                
  call dbgmsg "Sending mqtt to uri: "mqttserv                                   
  call dbgmsg "Subject:" subj                                                   
                                                                                
  /* Get text */                                                                
  'pipe safe 'safename' | stem msgtext. '                                       
                                                                                
  /* Compose test */                                                            
  txt = ""                                                                      
  sep = " " /* sep - separator, optional crlf */                                
  do i = 1 to msgtext.0                                                         
    txt = txt || msgtext.i || sep                                               
  end                                                                           
                                                                                
  return txt                                                                    
                                                                                
/* ******************************************** */                              
/*  Reading from NetView SAFE source            */                              
/* ******************************************** */                              
channel_stck:                                                                   
  parse arg eol                                                                 
  content = ''                                                                  
  do i = 1 to queued()                                                          
    parse pull line                                                             
    content = content || line || eol                                            
  end                                                                           
  return content                                                                
                                                                                
/* ******************************************** */                              
/*  Reading from NetView SAFE source            */                              
/* ******************************************** */                              
channel_file:                                                                   
  parse arg filename                                                            
                                                                                
  say "Channel FILE not implemented!"                                           
                                                                                
  env = ADDRESS()                                                               
  ADDRESS TSO                                                                   
  'ALLOC FILE(MQTTINP) DSN('filename') SHR'                                     
  'EXECIO * DISKR MQTTINP ( FIFO OPEN FINIS )'                                  
  content = channel_stck()                                                      
  'FREE  FILE(MQTTINP)'                                                         
                                                                                
  ADDRESS env                                                                   
                                                                                
  return content                                                                
                                                                                

