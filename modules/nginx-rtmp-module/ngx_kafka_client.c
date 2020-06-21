/*
 * Copyright (c) 2018 NVIDIA Corporation.  All rights reserved.
 *
 * NVIDIA Corporation and its licensors retain all intellectual property
 * and proprietary rights in and to this software, related documentation
 * and any modifications thereto.  Any use, reproduction, disclosure or
 * distribution of this software and related documentation without an express
 * license agreement from NVIDIA Corporation is strictly prohibited.
 *
 */
/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2017, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * kafka client based on Simple Apache Kafka producer from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <ngx_core.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdlib.h>
#include <glib.h>
#include <assert.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <netdb.h>

#include "rdkafka.h"
#include "ngx_kafka_client.h"

/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *opaque) {
}

typedef struct {
   rd_kafka_t *producer;         /* Producer instance handle */
   rd_kafka_topic_t *topic;  /* Topic object */
   rd_kafka_conf_t *conf;  /* Temporary configuration object */
   char topic_name[255];
} NvDsKafkaClientHandle;


/*
 * The kafka protocol adaptor expects the client to manage handle usage and retirement.
 * Specifically, client has to ensure that once a handle is retired through disconnect,
 * that it does not get used for either send or do_work.
 * While the library iplements a best effort mechanism to ensure that calling into these
 * functions with retired handles is  gracefully handled, this is not done in a thread-safe
 * manner
 *
 * Also, note thatrdkafka is inherently thread safe and therefore there is no need to implement separate
 * locking mechanisms in the kafka protocol adaptor for methods calling directly in rdkafka
 *
 */
void *nvds_kafka_client_init(char *brokers, char *topic) {
   NvDsKafkaClientHandle *kh = NULL;
   rd_kafka_conf_t *conf;  /* Temporary configuration object */
   char errstr[512];

  //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_INFO, "Connecting to kafka broker: %s on topic %s\n", brokers, topic);

   if ((brokers == NULL) || (topic == NULL)) {
    //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "Broker and/or topic is null. init failed\n");
     return NULL;
   }


   /*
     Create Kafka client configuration place-holder
   */
   conf = rd_kafka_conf_new();

   /* Set bootstrap broker(s) as a comma-separated list of
          host or host:port (default port 9092).
    * librdkafka will use the bootstrap brokers to acquire the full
          set of brokers from the cluster. */
   if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "Error connecting kafka broker: %s\n",errstr);
       return NULL;
    }

    /* Set the delivery report callback.
     * This callback will be called once per message to inform
         the application if delivery succeeded or failed.

     * See dr_msg_cb() above. */
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    kh = (NvDsKafkaClientHandle *)(malloc(sizeof(NvDsKafkaClientHandle)));
     if (kh == NULL) {
       //free rkt, rk, conf
       return NULL;
     }

     kh->producer = NULL;
     kh->topic = NULL;
     kh->conf = conf;
     snprintf(kh->topic_name, sizeof(kh->topic_name), "%s",topic);
     return (void *)kh;
}

//There could be several synchronous and asychronous send operations in flight.
//Once a send operation callback is received the course of action  depends on if it's sync or async
// -- if it's sync then the associated completion flag should  be set
// -- if it's asynchronous then completion callback from the user should be called along with context
NvDsMsgApiErrorType nvds_kafka_client_send(void *kv,  const uint8_t *payload, int len, int sync, void *ctx, nvds_msgapi_send_cb_t cb, char *key, int keylen)
{
  NvDsKafkaClientHandle *kh = (NvDsKafkaClientHandle *)kv;
  uint8_t done = 0;

  // NvDsKafkaSendCompl *scd;
  // if (sync) {
  //   NvDsKafkaSyncSendCompl *sc= new NvDsKafkaSyncSendCompl(&done);
  //   scd = sc;
  // } else {
  //   NvDsKafkaAsyncSendCompl *sc= new NvDsKafkaAsyncSendCompl(ctx, cb);
  //   scd = sc;
  // }

  if (!kh) {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "send called on NULL handle \n");
    return NVDS_MSGAPI_ERR;
  }

  if (rd_kafka_produce(
          /* Topic object */
          kh->topic,
          /* Use builtin partitioner to select partition*/
          RD_KAFKA_PARTITION_UA,
          /* Make a copy of the payload. */
          RD_KAFKA_MSG_F_COPY,
          /* Message payload (value) and length */
          (void *)payload, len,
          /* Optional key and its length */
          key, keylen,
          /* Message opaque, provided in
           * delivery report callback as
           * msg_opaque. */
          NULL) == -1) {
       return NVDS_MSGAPI_ERR;
     }
     else
     {
        // FILE* fp = NULL;
        // fp = fopen("/data/record/callback.txt", "a+");
        // fputs((char*)payload, fp);
        // fclose(fp);
      //  NvDsMsgApiErrorType err;
       if  (!sync)
          return NVDS_MSGAPI_OK;
       else
       {
          while (sync && !done) {
            usleep(1000);
            rd_kafka_poll(kh->producer, 0/*non-blocking*/);
          }
          // err = (scd)->get_err();
	        return NVDS_MSGAPI_OK;
        }
     }
}

NvDsMsgApiErrorType nvds_kafka_client_setconf(void *kv, char *key, char *val)
{
  char errstr[512];
  NvDsKafkaClientHandle *kh = (NvDsKafkaClientHandle *)kv;
  
  if (rd_kafka_conf_set(kh->conf, key, val , errstr, sizeof(errstr)) \
           != RD_KAFKA_CONF_OK) {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "Error setting config setting %s; %s\n", key, errstr );
    return NVDS_MSGAPI_ERR;
  } else {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_INFO, "set config setting %s to %s\n", key, val);
    return NVDS_MSGAPI_OK;
  }
}

/**
  Instantiates the rd_kafka_t object, which initializes the protocol
 */
NvDsMsgApiErrorType nvds_kafka_client_launch(void *kv)
{
   rd_kafka_t *rk;         /* Producer instance handle */
   rd_kafka_topic_t *rkt;  /* Topic object */
   NvDsKafkaClientHandle *kh = (NvDsKafkaClientHandle *)kv;
   char errstr[512];

   /*
      * Create producer instance.
      * NOTE: rd_kafka_new() takes ownership of the conf object
      *       and the application must not reference it again after
      *       this call.
    */
   rk = rd_kafka_new(RD_KAFKA_PRODUCER, kh->conf, errstr, sizeof(errstr));
   if (!rk) {
      // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "Failed to create new producer: %s\n", errstr);
      return NVDS_MSGAPI_ERR;
   }

    /* Create topic object that will be reused for each message
         * produced.
         *
         * Both the producer instance (rd_kafka_t) and topic objects (topic_t)
         * are long-lived objects that should be reused as much as possible.
    */
   rkt = rd_kafka_topic_new(rk, kh->topic_name, NULL);
   if (!rkt) {
        // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "Failed to create topic object: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk);
        return NVDS_MSGAPI_ERR;
   }
   kh->producer = rk;
   kh->topic = rkt;
   return NVDS_MSGAPI_OK;

}

void nvds_kafka_client_finish(void *kv)
{
  NvDsKafkaClientHandle *kh = (NvDsKafkaClientHandle *)kv;

  if (!kh) {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "finish called on NULL handle\n");
    return;
  }
    
  rd_kafka_flush (kh->producer, 10000);

  /* Destroy topic object */
  rd_kafka_topic_destroy(kh->topic);

  /* Destroy the producer instance */
  rd_kafka_destroy( kh->producer );

}

void nvds_kafka_client_poll(void *kv)
{
  NvDsKafkaClientHandle *kh = (NvDsKafkaClientHandle *)kv;
  if (kh)
    rd_kafka_poll(kh->producer, 0/*non-blocking*/);
}

#define MAX_FIELD_LEN 255 //maximum topic length supported by kafka is 255

#define NVDS_MSGAPI_VERSION "1.0"

#define CONFIG_GROUP_MSG_BROKER "message-broker"
#define CONFIG_GROUP_MSG_BROKER_RDKAFKA_CFG "proto-cfg"
#define CONFIG_GROUP_MSG_BROKER_PARTITION_KEY "partition-key"


// int json_get_key_value(const char *msg, int msglen, const char *key, char *value, int nbuf);

typedef struct {
  void *kh;
  char topic[MAX_FIELD_LEN];
  char partition_key_field[MAX_FIELD_LEN];
} NvDsKafkaProtoConn;

/**
 * internal function to read settings from config file
 * Documentation needs to indicate that kafka config parameters are:
  (1) located within application level config file passed to connect
  (2) within the message broker group of the config file
  (3) specified based on 'rdkafka-cfg' key
  (4) the various options to rdkafka are specified based on 'key=value' format, within various entries semi-colon separated
Eg:
[message-broker]
enable=1
broker-proto-lib=/opt/nvidia/deepstream/deepstream-<version>/lib/libnvds_kafka_proto.so
broker-conn-str=kafka1.data.nvidiagrid.net;9092;metromind-test-1
rdkafka-cfg="message.timeout.ms=2000"

 */
static void nvds_kafka_read_config(void *kh, char *config_path, char *partition_key_field, int field_len)
{
  //iterate over the config params to set one by one
  //finally call into launch function passing topic
  GKeyFile *key_file = g_key_file_new ();
  gchar **keys = NULL;
  gchar **key = NULL;
  GError *error = NULL;
  char *confptr = NULL, *curptr = NULL;

  if (!g_key_file_load_from_file (key_file, config_path, G_KEY_FILE_NONE,
            &error)) {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR,  "unable to load config file at path %s; error message = %s\n", config_path, error->message);
    return;
  }

  keys = g_key_file_get_keys(key_file, CONFIG_GROUP_MSG_BROKER, NULL, &error);
  if (error) {
    //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR,  "Error parsing config file. %s\n", error->message);
     return;
  }
  for (key = keys; *key; key++) {
    gchar *setvalquote;

    // check if this is one of adaptor settings
    if (!g_strcmp0(*key, CONFIG_GROUP_MSG_BROKER_RDKAFKA_CFG))
	{
           setvalquote = g_key_file_get_string (key_file, CONFIG_GROUP_MSG_BROKER,
               CONFIG_GROUP_MSG_BROKER_RDKAFKA_CFG, &error);

	   if (error) {
            //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR,  "Error parsing config file\n");
	     return;
	   }

	   confptr = setvalquote;
	   size_t conflen = strlen(confptr);

	   //remove "". (string length needs to be at least 2)
	   //Could use g_shell_unquote but it might have other side effects
	   if ((conflen <3) || (confptr[0] != '"') || (confptr[conflen-1] != '"')) {
            //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR,  "invalid format for rdkafa config entry. Start and end with \"\"\n");
	     return;
           }
           confptr[conflen-1] = '\0'; //remove ending quote
           confptr = confptr + 1; //remove starting quote
	  //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_INFO,  "kafka setting %s = %s\n", *key, confptr);
	}

       // check if this entry specifies the partition key field
    if (!g_strcmp0(*key, CONFIG_GROUP_MSG_BROKER_PARTITION_KEY))
        {
           gchar *key_name_conf;
           key_name_conf = g_key_file_get_string (key_file, CONFIG_GROUP_MSG_BROKER,
                CONFIG_GROUP_MSG_BROKER_PARTITION_KEY, &error);
           if (error) {
            //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR,  "Error parsing config file\n");
             g_error_free(error);
             return;
           }
           strncpy(partition_key_field, (char *)key_name_conf, field_len);
          //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_INFO,  "kafka partition key field name = %s\n", partition_key_field);
           g_free(key_name_conf);
        }
  }

  if (!confptr) {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_DEBUG,  "No " CONFIG_GROUP_MSG_BROKER_RDKAFKA_CFG " entry found in config file.\n");
    return;
 }
  
  char *equalptr, *semiptr;
  int keylen, vallen, conflen = strlen(confptr);
  char confkey[MAX_FIELD_LEN], confval[MAX_FIELD_LEN];
  curptr = confptr;

  while (((equalptr = strchr(curptr, '=')) != NULL) && ((curptr - confptr) < conflen)) {
     keylen = (equalptr - curptr);
     if (keylen >= MAX_FIELD_LEN)
       keylen = MAX_FIELD_LEN - 1;

     memcpy(confkey, curptr, keylen);
     confkey[keylen] = '\0';

     if (equalptr >= (confptr + conflen)) //no more string; dangling key
       return;

     semiptr = strchr(equalptr+1, ';');

     if (!semiptr) {
       vallen = (confptr + conflen - equalptr - 1);//end of strng case
       curptr = (confptr + conflen);
     }
     else {
       curptr = semiptr + 1;
       vallen = (semiptr - equalptr - 1);
     }

     if (vallen >= MAX_FIELD_LEN)
       vallen = MAX_FIELD_LEN - 1;

     memcpy(confval, (equalptr + 1), vallen);
     confval[vallen] = '\0';

     nvds_kafka_client_setconf(kh, confkey, confval);

  }
}


/**
 * connects to a broker based on given url and port to check if address is valid
 * Returns 0 if valid, and non-zero if invalid.
 * Also returns 0 if there is trouble resolving  the address or creating connection
 */
static int test_kafka_broker_endpoint(char *burl, char *bport) {
  int sockid;
  int port = atoi(bport);
  int flags;
  fd_set wfds;
  int error;
  struct addrinfo *res, hints;

  if (!port)
    return -1;

  memset(&hints, 0, sizeof (hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  //resolve the given url
  if ((error = getaddrinfo(burl, bport, &hints, &res))) {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "getaddrinfo returned error %d\n", error);

    if ((error == EAI_FAIL) || (error == EAI_NONAME) ||  (error == EAI_NODATA)){
      // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "count not resolve addr - permanent failure\n");
      return error; //permanent failure to resolve
    }
    else 
      return 0; //unknown error during resolve; can't invalidate address
  }

  //iterate through all ip addresses resolved for the url
  for(; res != NULL; res = res->ai_next) {
    sockid = socket(AF_INET, SOCK_STREAM, 0); //tcp socket

    //make socket non-blocking
    flags = fcntl(sockid, F_GETFL);
    if (fcntl(sockid, F_SETFL, flags | O_NONBLOCK) == -1)
      /* having trouble making socket non-blocking;
        can't check network address, and so assume it is valid
      */
     return 0;

    if (!connect(sockid, (struct sockaddr *)res->ai_addr, res->ai_addrlen)) {
      return 0; //connection succeeded right away
    }
    else {
      if (errno == EINPROGRESS) { //normal for non-blocking socker
        struct timeval conn_timeout;
        int optval;
        socklen_t optlen;

        conn_timeout.tv_sec = 5; //give 5 sec for connection to go through
        conn_timeout.tv_usec = 0;
        FD_ZERO(&wfds);
        FD_SET(sockid, &wfds);

        int err = select(sockid+1, NULL, & wfds, NULL, &conn_timeout);
        switch(err) {
          case 0: //timeout
            return ETIMEDOUT;

          case 1: //socket unblocked; now figure out why
            optval = -1;
            optlen = sizeof(optval);
            if (getsockopt(sockid, SOL_SOCKET, SO_ERROR, &optval, &optlen) == -1) {
              /* error getting socket options; can't invalidate address */
               return 0;
            }
            if (optval == 0)
              return 0; //no error; connection succeeded
            else
              return optval;	 //connection failed; something wrong with address

          case -1: //error in select, can't invalidate address
            return 0;
        }
      } else
        return 0; // error in connect; can't invalidate address
    } // non-blocking connect did not succeed
  }
  return 0; //if we got here then can't invalidate
}


/**
 * Connects to a remote kafka broker based on connection string.
 */
NvDsMsgApiHandle nvds_msgapi_connect(char *connection_str,  nvds_msgapi_connect_cb_t connect_cb, char *config_path)
{
  NvDsKafkaProtoConn *conn_ptr = (NvDsKafkaProtoConn *)malloc(sizeof(NvDsKafkaProtoConn));
  char burl[MAX_FIELD_LEN], bport[MAX_FIELD_LEN], btopic[MAX_FIELD_LEN], brokerurl[MAX_FIELD_LEN * 2];
  int urllen, portlen, topiclen;
  urllen = portlen = topiclen = MAX_FIELD_LEN;
  char *portptr, *topicptr;

//   nvds_log_open();
  // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_INFO, "nvds_msgapi_connect:connection_str = %s\n", connection_str);

  portptr = strchr(connection_str, ';');

  if (conn_ptr == NULL) { //malloc failed
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "Unable to allocate memory for kafka connection handle. Can't create connection\n");
    return NULL;
  }

  if (portptr == NULL) { //invalid format
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "invalid connection string format. Can't create connection\n");
    return NULL;
  }

  topicptr = strchr(portptr+1, ';');

  if (topicptr == NULL) { //invalid format
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "invalid connection string format. Can't create connection\n");
    return NULL;
  }

  urllen = (portptr - connection_str);
  if (urllen >= MAX_FIELD_LEN)
    urllen = MAX_FIELD_LEN - 1;

  memcpy(burl, connection_str, urllen);
  burl[urllen] = '\0';
  
  portlen = (topicptr - portptr - 1);
  if (portlen >= MAX_FIELD_LEN)
    portlen = MAX_FIELD_LEN - 1;
  memcpy(bport, (portptr + 1), portlen);
  bport[portlen] = '\0';

  topiclen = (strlen(connection_str) - urllen - portlen - 2);
  if (topiclen >=  MAX_FIELD_LEN)
    topiclen = MAX_FIELD_LEN;
  memcpy(btopic, (topicptr + 1), topiclen);
  btopic[topiclen] = '\0';

  // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_INFO, "kafka broker url = %s; port = %s; topic = %s", burl, bport, btopic);

  snprintf(brokerurl, sizeof(brokerurl), "%s:%s", burl, bport);

  if (test_kafka_broker_endpoint(burl, bport)) {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "Invalid address or network endpoint down. kafka connect failed\n");
    free(conn_ptr);
    return NULL;
  }

  conn_ptr->kh = nvds_kafka_client_init(brokerurl, btopic);
  if (!conn_ptr->kh) {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "Unable to init kafka client.\n");
    free(conn_ptr);
    return NULL;
  }
  strncpy(conn_ptr->topic, btopic, MAX_FIELD_LEN);

  /* set key field name to default value of sensor.id */
  // strncpy(conn_ptr->partition_key_field, "sensor.id", sizeof(conn_ptr->partition_key_field));

  if (config_path) {
    nvds_kafka_read_config(conn_ptr->kh, config_path, conn_ptr->partition_key_field, sizeof(conn_ptr->partition_key_field));
  }
  if (nvds_kafka_client_launch(conn_ptr->kh) != NVDS_MSGAPI_OK) {
    free(conn_ptr);
    return NULL;
  }

  return (NvDsMsgApiHandle)(conn_ptr);
}

//There could be several synchronous and asychronous send operations in flight.
//Once a send operation callback is received the course of action  depends on if it's synch or async
// -- if it's sync then the associated complletion flag should  be set
// -- if it's asynchronous then completion callback from the user should be called
NvDsMsgApiErrorType nvds_msgapi_send(NvDsMsgApiHandle h_ptr, char *topic, const uint8_t *payload, size_t nbuf)
{
  char idval[100];
  int retval;

  // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_DEBUG, "nvds_msgapi_send: payload=%.*s, \n topic = %s, h->topic = %s\n", nbuf, payload, topic, (((NvDsKafkaProtoConn *) h_ptr)->topic));

  if (strcmp(topic, (((NvDsKafkaProtoConn *) h_ptr)->topic))) {
    //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "nvds_msgapi_send: send topic has to match topic defined at connect.\n");
     return NVDS_MSGAPI_ERR;
  }

  // parition key retrieved from config file
  // char *partition_key_field = ((NvDsKafkaProtoConn *) h_ptr)->partition_key_field;
  // retval = json_get_key_value((const char *)payload, nbuf, partition_key_field , idval, sizeof(idval));
  retval = 0;

  if (retval)
    return nvds_kafka_client_send(((NvDsKafkaProtoConn *) h_ptr)->kh, payload, nbuf, 1, NULL, NULL, idval, strlen(idval));
  else {
      // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "nvds_msgapi_send: no matching json field found based on kafka key config; using default partition\n");

       return nvds_kafka_client_send(((NvDsKafkaProtoConn *) h_ptr)->kh, payload, nbuf, 1, NULL, NULL, NULL, 0);
  }
}

NvDsMsgApiErrorType nvds_msgapi_send_async(NvDsMsgApiHandle h_ptr, char *topic, const uint8_t *payload, size_t nbuf,  nvds_msgapi_send_cb_t send_callback, void *user_ptr)
{
  char idval[100];
  int retval;

  // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_DEBUG, "nvds_msgapi_send_async: payload=%.*s, \n topic = %s, h->topic = %s\n", nbuf, payload, topic, (((NvDsKafkaProtoConn *) h_ptr)->topic));

  if (strcmp(topic, (((NvDsKafkaProtoConn *) h_ptr)->topic))) {
    //  ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "nvds_msgapi_send_async: send topic has to match topic defined at connect.\n");
     return NVDS_MSGAPI_ERR;
  }

  // parition key retrieved from config file
  // char *partition_key_field = ((NvDsKafkaProtoConn *) h_ptr)->partition_key_field;
  // retval = json_get_key_value((const char *)payload, nbuf, partition_key_field , idval, sizeof(idval));
  retval = 0;
  if (retval)
    return nvds_kafka_client_send(((NvDsKafkaProtoConn *) h_ptr)->kh, payload, nbuf, 0, user_ptr, \
             send_callback, idval, strlen(idval));
  else {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_ERR, "no matching json field found based on kafka key config; using default partition\n");
    return nvds_kafka_client_send(((NvDsKafkaProtoConn *) h_ptr)->kh, payload, nbuf, 0, user_ptr, \
                  send_callback, NULL, 0);

  }

}

void nvds_msgapi_do_work(NvDsMsgApiHandle h_ptr)
{
  // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_DEBUG, "nvds_msgapi_do_work\n");
  nvds_kafka_client_poll(((NvDsKafkaProtoConn *) h_ptr)->kh);
}

NvDsMsgApiErrorType nvds_msgapi_disconnect(NvDsMsgApiHandle h_ptr)
{
  if (!h_ptr) {
    // ngx_log_error(NVDS_KAFKA_LOG_CAT, NGX_LOG_DEBUG, "nvds_msgapi_disconnect called with null handle\n");
    return NVDS_MSGAPI_OK;
  }

  nvds_kafka_client_finish(((NvDsKafkaProtoConn *) h_ptr)->kh);
  (((NvDsKafkaProtoConn *) h_ptr)->kh) = NULL;
//   nvds_log_close();
  return NVDS_MSGAPI_OK;
}

/**
  * Returns version of API supported byh this adaptor
  */
char *nvds_msgapi_getversion()
{
  return (char *)NVDS_MSGAPI_VERSION;
}