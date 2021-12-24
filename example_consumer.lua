local BROKERS_ADDRESS = { 'localhost' }
local TOPIC_NAMES = {'test_topic'}
local io = require('io')
local config = require('rdkafka.config').create()

config['client.id'] = 'test-consumer'
config['group.id'] = 'test-consumer-group'
config['enable.auto.commit'] = true
config['auto.commit.interval.ms'] = 5000
config['statistics.interval.ms'] = 60000
config['bootstrap.servers'] = 'localhost:9092'

local consumer = require('rdkafka.consumer').create(config)

consumer:subscribe(TOPIC_NAMES)
for i=1,#TOPIC_NAMES do
  print('Subscribed to Topic="'..TOPIC_NAMES[i]..'"\n')
end

while(true) do
  consumer:poll(1000, function(msg)
    if(msg.err ~= 0) then
      print('Error='..msg.err..' Description="'..msg.errstr..'"')
      return
    end
    print('Topic="'..msg.topic..'" Partition='..msg.partition..' Offset='..msg.offset..' Key="'..msg.key..'" Payload="'..msg.payload..'"')
  end)
end
