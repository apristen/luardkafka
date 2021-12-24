local librdkafka = require 'rdkafka.librdkafka'
local KafkaConfig = require 'rdkafka.config'
local KafkaTopic = require 'rdkafka.topic'
local ffi = require 'ffi'

local DEFAULT_DESTROY_TIMEOUT_MS = 3000

local KafkaConsumer = {}
KafkaConsumer.__index = KafkaConsumer

function KafkaConsumer.create(kafka_config, destroy_timeout_ms)
    local config = nil
    if kafka_config ~= nil then
        config = KafkaConfig.create(kafka_config).kafka_conf_
        ffi.gc(config, nil)
    end

    local ERRLEN = 256
    local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected
    local kafka = librdkafka.rd_kafka_new(librdkafka.RD_KAFKA_CONSUMER, config, errbuf, ERRLEN)

    if kafka == nil then
        error(ffi.string(errbuf))
    end

    local consumer = {kafka_ = kafka}
    KafkaTopic.kafka_topic_map_[kafka] = {}

    setmetatable(consumer, KafkaConsumer)
    ffi.gc(consumer.kafka_, function (...)
        for k, topic_ in pairs(KafkaTopic.kafka_topic_map_[consumer.kafka_]) do
            librdkafka.rd_kafka_topic_destroy(topic_) 
        end
        KafkaTopic.kafka_topic_map_[consumer.kafka_] = nil
        librdkafka.rd_kafka_destroy(...)
        librdkafka.rd_kafka_wait_destroyed(destroy_timeout_ms or DEFAULT_DESTROY_TIMEOUT_MS)
        end
    )

    return consumer
end

function KafkaConsumer:subscribe(topics)
    assert(self.kafka_ ~= nil)
    assert(type(topics) == 'table') -- array of topics
    local topics_list = librdkafka.rd_kafka_topic_partition_list_new(1)
    for i=1,#topics do
      librdkafka.rd_kafka_topic_partition_list_add(topics_list, topics[i], -1)
    end
    return librdkafka.rd_kafka_subscribe(self.kafka_, topics_list)
end

function KafkaConsumer:brokers_add(broker_list)
    assert(self.kafka_ ~= nil)
    return librdkafka.rd_kafka_brokers_add(self.kafka_, broker_list)
end

function KafkaConsumer:poll(timeout_ms, cb)
    assert(self.kafka_ ~= nil)
    local msg = librdkafka.rd_kafka_consumer_poll(self.kafka_, timeout_ms)
    
    if(msg == ffi.NULL) then return end -- no messages during poll
    
    if(cb) then
      local luamsg = {
        err = tonumber(msg.err),
        errstr = ffi.string(librdkafka.rd_kafka_err2str(msg.err)),
        topic = ffi.string(librdkafka.rd_kafka_topic_name(msg.rkt)),
        partition = tonumber(msg.partition),
        offset = tonumber(msg.offset),
        key = ffi.string(msg.key, msg.key_len),
        payload = ffi.string(msg.payload, msg.len)
      }
      cb(luamsg)
    end

    librdkafka.rd_kafka_message_destroy(msg)
end
jit.off(KafkaConsumer.poll)

return KafkaConsumer
