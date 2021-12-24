local BROKERS_ADDRESS = { 'localhost' }
local TOPIC_NAME = 'test_topic'
local KAFKA_PARTITION_UA = -1
local io = require('io')
local config = require('rdkafka.config').create()

config['statistics.interval.ms'] =  '100'
config:set_delivery_cb(function (payload, err) print('Delivery Callback: payload="'..payload..'" err="'..tostring(err)..'"') end)
--config:set_stat_cb(function (payload) print('Stat Callback: payload="'..payload..'"') end)

local producer = require 'rdkafka.producer'.create(config)

for k,v in pairs(BROKERS_ADDRESS) do
  producer:brokers_add(v)
end

local topic_config = require 'rdkafka.topic_config'.create()
topic_config['auto.commit.enable'] = 'true'

local topic = require 'rdkafka.topic'.create(producer, TOPIC_NAME, topic_config)

io.write('Producing on topic "' .. TOPIC_NAME .. '". type line "q" to quit.\n')
io.flush()

while(true) do
  local msg = io.read()
  if(msg == 'q') then break end

  producer:produce(topic, KAFKA_PARTITION_UA, msg)
  while producer:outq_len() ~= 0 do
    producer:poll(10)
  end
end
