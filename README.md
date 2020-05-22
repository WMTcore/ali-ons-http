# ali-ons-http
基于阿里云mq http sdk，参考部分ali-ons代码

## Install

```bash
npm install ali-ons-egg --save
```

## Usage

consumer

```js
'use strict';

const Consumer = require('ali-ons-http').Consumer;
const consumer = new Consumer({
  accessKeyId: 'your-accessKeyId',
  accessKeySecret: 'your-AccessKeySecret',
  consumerGroup: 'your-consumer-group',
  // onsAddr: '', // ons http的接入地址
  // instanceId:'', // 公网实例不要填,会报权限问题
});

consumer.subscribe(config.topic, '*', async msg => {
  console.log(`receive message, msgId: ${msg.msgId}, body: ${msg.body.toString()}`)
});

consumer.on('error', err => console.log(err));
```

producer

```js
'use strict';
const Producer = require('ali-ons-http').Producer;
const Message = require('ali-ons-http').Message;

const producer = new Producer({
  accessKeyId: 'your-accessKeyId',
  accessKeySecret: 'your-AccessKeySecret',
  producerGroup: 'your-producer-group',
  // onsAddr: '', // ons http的接入地址
  // instanceId:'', // 公网实例不要填,会报权限问题
});

(async () => {
  const msg = new Message('your-topic', // topic
    'TagA', // tag
    'Hello ONS !!! ' // body
  );

  // set Message#keys
  msg.keys = ['key1'];

  // delay consume
  // msg.setStartDeliverTime(Date.now() + 5000);

  const sendResult = await producer.send(msg);
  console.log(sendResult);
})().catch(err => console.error(err))
```