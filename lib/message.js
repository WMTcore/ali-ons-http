'use strict';

const { MessageProperties } = require('@aliyunmq/mq-http-sdk');

class Message extends MessageProperties {

  /**
   * 创建消息对象
   * @param {String} topic -
   * @param {String} tags -
   * @param {String|Buffer} body -
   * @param {Object} options -
   * @class
   */
  constructor(topic, tags, body) {
    super();
    if (arguments.length === 2) {
      body = tags;
      tags = null;
    }

    this.topic = topic;
    this.properties = {};
    this.msgId = null;
    this.tags = tags;
    this.body = body;
  }

  /**
   * 消息关键词
   * @property {String} Message#keys
   */
  get keys() {
    return this.properties && this.properties.KEYS;
  }

  /**
   * 设置消息KEY
   * @param {string} key 消息KEY
   */
  set keys(key) {
    this.properties.KEYS = key.trim();
  }

  /**
   * 延迟执行的时间点，毫秒
   * @param {number} delayTime 延迟执行目标时间戳
   */
  setStartDeliverTime(delayTime) {
    this.startDeliverTime(delayTime);
  }

  /**
   * 获取消息延迟时间
   * @return {number} 延迟执行目标时间戳
   */
  getStartDeliverTime() {
    return this.properties.__STARTDELIVERTIME || 0;
  }

  setTransCheckImmunityTime(timeSeconds) {
    this.transCheckImmunityTime(timeSeconds);
  }

  getTransCheckImmunityTime() {
    return this.properties.__TransCheckT || 0;
  }
}

module.exports = Message;
