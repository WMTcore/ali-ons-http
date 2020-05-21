'use strict';

const Base = require('sdk-base');
const logger = require('./logger');
const { MQClient } = require('@aliyunmq/mq-http-sdk');

const defaultOptions = {
  logger,
  sendMsgTimeout: 3000,
  compressMsgBodyOverHowmuch: 1024 * 4,
  retryTimesWhenSendFailed: 3,
  // maxMessageSize: 1024 * 128,
};

class MQProducer extends Base {
  constructor(options) {
    super();
    this.options = Object.assign(defaultOptions, options);
    const { accessKey, secretKey, onsAddr, instanceId } = this.options;
    this._mqClient = new MQClient(onsAddr, accessKey, secretKey);
    this.instanceId = instanceId || '';
  }

  get logger() {
    return this.options.logger;
  }

  async send(msg) {
    const { topic, tags, body } = msg;
    const maxTimeout = this.options.sendMsgTimeout + 1000;
    const timesTotal = 1 + this.options.retryTimesWhenSendFailed;
    const beginTimestamp = Date.now();
    let times = 0;
    let sendResult;
    const producer = this._mqClient.getProducer(this.instanceId, topic);
    for (; times < timesTotal && Date.now() - beginTimestamp < maxTimeout; times++) {
      try {
        sendResult = await producer.publishMessage(body, tags, msg);
        if (sendResult.body.MessageId) {
          break;
        }
      } catch (err) {
        this.logger.error(err);
      }
    }
    if (!sendResult) {
      throw new Error(`Send [${times}] times, still failed, cost [${Date.now() - beginTimestamp}]ms, Topic: ${msg.topic}`);
    }
    const { MessageId, MessageBodyMD5 } = sendResult.body;
    return {
      msgId: MessageId,
      msgBodyMD5: MessageBodyMD5,
    };
  }
}

module.exports = MQProducer;
