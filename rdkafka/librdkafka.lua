
local ffi = require 'ffi'

ffi.cdef[[
    typedef struct rd_kafka_s rd_kafka_t;
    typedef struct rd_kafka_conf_s rd_kafka_conf_t;
    typedef struct rd_kafka_topic_s rd_kafka_topic_t;
    typedef struct rd_kafka_topic_conf_s rd_kafka_topic_conf_t;

    typedef enum rd_kafka_type_t {
      RD_KAFKA_PRODUCER,
      RD_KAFKA_CONSUMER
    } rd_kafka_type_t;

    typedef enum {
      RD_KAFKA_RESP_ERR__BEGIN = -200,
      RD_KAFKA_RESP_ERR_NO_ERROR = 0,
      /* ... */
    } rd_kafka_resp_err_t;

    typedef enum {
      RD_KAFKA_CONF_UNKNOWN = -2, /* Unknown configuration name. */
      RD_KAFKA_CONF_INVALID = -1, /* Invalid configuration value. */
      RD_KAFKA_CONF_OK = 0        /* Configuration okay */
    } rd_kafka_conf_res_t;

    typedef struct rd_kafka_message_s {
      rd_kafka_resp_err_t err;
      rd_kafka_topic_t *rkt;
      int32_t partition;
      void   *payload;
      size_t  len;
      void   *key;
      size_t  key_len;
      int64_t offset;
      void  *_private;
    } rd_kafka_message_t;
    
    typedef struct rd_kafka_topic_partition_s {
      char        *topic;             
      int32_t      partition;         
      int64_t      offset;            
      void        *metadata;          
      size_t       metadata_size;     
      void        *opaque;            
      rd_kafka_resp_err_t err;        
      void       *_private;           
    } rd_kafka_topic_partition_t;

    typedef struct rd_kafka_topic_partition_list_s {
      int cnt;               
      int size;              
      rd_kafka_topic_partition_t *elems; 
    } rd_kafka_topic_partition_list_t;
    
    rd_kafka_conf_t *rd_kafka_conf_new (void);
    rd_kafka_conf_t *rd_kafka_conf_dup (const rd_kafka_conf_t *conf);
    void rd_kafka_conf_destroy (rd_kafka_conf_t *conf);
    const char **rd_kafka_conf_dump (rd_kafka_conf_t *conf, size_t *cntp);
    void rd_kafka_conf_dump_free (const char **arr, size_t cnt);
    rd_kafka_conf_res_t rd_kafka_conf_set (rd_kafka_conf_t *conf, const char *name, const char *value, char *errstr, size_t errstr_size);
    
    void rd_kafka_conf_set_consume_cb (rd_kafka_conf_t *conf, void(*consume_cb)(rd_kafka_message_t *rkmessage, void *opaque));
    void rd_kafka_conf_set_dr_cb (rd_kafka_conf_t *conf, void (*dr_cb) (rd_kafka_t *rk, void *payload, size_t len, rd_kafka_resp_err_t err, void *opaque, void *msg_opaque));
    void rd_kafka_conf_set_error_cb (rd_kafka_conf_t *conf, void  (*error_cb) (rd_kafka_t *rk, int err, const char *reason, void *opaque));
    void rd_kafka_conf_set_stats_cb (rd_kafka_conf_t *conf, int (*stats_cb) (rd_kafka_t *rk, char *json, size_t json_len, void *opaque));
    void rd_kafka_conf_set_log_cb (rd_kafka_conf_t *conf, void (*log_cb) (const rd_kafka_t *rk, int level, const char *fac, const char *buf));

    rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, rd_kafka_conf_t *conf, char *errstr, size_t errstr_size);
    void rd_kafka_destroy (rd_kafka_t *rk);
    int rd_kafka_brokers_add (rd_kafka_t *rk, const char *brokerlist);

    rd_kafka_topic_conf_t *rd_kafka_topic_conf_new (void);
    rd_kafka_topic_conf_t *rd_kafka_topic_conf_dup (const rd_kafka_topic_conf_t *conf);
    rd_kafka_conf_res_t rd_kafka_topic_conf_set (rd_kafka_topic_conf_t *conf, const char *name, const char *value, char *errstr, size_t errstr_size);
    void rd_kafka_topic_conf_destroy (rd_kafka_topic_conf_t *topic_conf);
    const char **rd_kafka_topic_conf_dump (rd_kafka_topic_conf_t *conf, size_t *cntp);

    rd_kafka_topic_t *rd_kafka_topic_new (rd_kafka_t *rk, const char *topic, rd_kafka_topic_conf_t *conf);
    const char *rd_kafka_topic_name (const rd_kafka_topic_t *rkt);
    void rd_kafka_topic_destroy (rd_kafka_topic_t *rkt);

    int rd_kafka_produce (rd_kafka_topic_t *rkt, int32_t partitition, int msgflags, void *payload, size_t len, const void *key, size_t keylen, void *msg_opaque);
    int rd_kafka_outq_len (rd_kafka_t *rk);
    int rd_kafka_poll (rd_kafka_t *rk, int timeout_ms);

    rd_kafka_topic_partition_list_t* rd_kafka_topic_partition_list_new (int size);
    rd_kafka_topic_partition_t* rd_kafka_topic_partition_list_add (rd_kafka_topic_partition_list_t *rktparlist, const char *topic, int32_t  partition);
    rd_kafka_resp_err_t rd_kafka_subscribe (rd_kafka_t *rk, const rd_kafka_topic_partition_list_t *topics);
    rd_kafka_message_t *rd_kafka_consumer_poll (rd_kafka_t *rk, int timeout_ms);
    rd_kafka_resp_err_t rd_kafka_commit (rd_kafka_t *rk, const rd_kafka_topic_partition_list_t *offsets, int async);
    void rd_kafka_message_destroy (rd_kafka_message_t *rkmessage);
    
    int rd_kafka_wait_destroyed (int timeout_ms);

    rd_kafka_resp_err_t rd_kafka_errno2err (int errnox);
    const char *rd_kafka_err2str (rd_kafka_resp_err_t err);
    int rd_kafka_thread_cnt (void);
]]

local librdkafka = ffi.load("librdkafka.so.1")
return librdkafka
