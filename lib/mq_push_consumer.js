'use strict';

const Base = require('sdk-base');
const assert = require('assert');
const is = require('is-type-of');
const logger = require('./logger');
const MixAll = require('./mix_all');
const sleep = require('mz-modules/sleep');
const { MQClient } = require('@aliyunmq/mq-http-sdk');

const defaultOptions = {
  logger,
  pullTimeDelayMillsWhenException: 3000, // 拉消息异常时，延迟一段时间再拉
  pullTimeDelayMillsWhenFlowControl: 5000, // 进入流控逻辑，延迟一段时间再拉
  consumerTimeoutMillisWhenSuspend: 15, // 长轮询模式，Consumer超时时间
  pullThresholdForQueue: 10, // 本地队列消息数超过此阀值，开始流控
  pullInterval: 0, // 拉取消息的频率, 如果为了降低拉取速度，可以设置大于0的值
  consumeMessageBatchMaxSize: 1, // 消费一批消息，最大数
  pullBatchSize: 5, // 拉消息，一次拉多少条
  parallelConsumeLimit: 1, // 并发消费消息限制
};

class MQPushConsumer extends Base {
  constructor(options) {
    super(Object.assign(defaultOptions, options));
    const { accessKey, secretKey, onsAddr, consumerGroup, instanceId } = this.options;
    this.consumerGroup = consumerGroup;
    this.instanceId = instanceId || '';
    this.subscriptions = new Map();
    this.processQueue = new Map();
    this._mqClient = new MQClient(onsAddr, accessKey, secretKey);

    // 订阅重试 TOPIC 目前仅支持集群消费模式
    // const retryTopic = MixAll.getRetryTopic(this.consumerGroup);
    // this.subscribe(retryTopic, '*', async msg => {
    //   const originTopic = msg.retryTopic;
    //   const originMsgId = msg.originMessageId;
    //   const subscription = this.subscriptions.get(originTopic) || {};
    //   const handler = subscription.handler;
    //   if (!MixAll.isRetryTopic(originTopic) && handler) {
    //     await handler(msg);
    //   } else {
    //     this.logger.warn('[MQPushConsumer] retry message no handler, originTopic: %s, originMsgId: %s, msgId: %s',
    //       originTopic,
    //       originMsgId,
    //       msg.msgId);
    //   }
    // });
  }

  get logger() {
    return this.options.logger;
  }

  _pullMessage(consumer, topic) {
    // 使用自执行async 避免一直同步等待消息拉取
    (async () => {
      // eslint-disable-next-line no-constant-condition
      while (true) {
        try {
          await this.executePullRequestImmediately(consumer, topic);
          await sleep(this.options.pullInterval);
        } catch (e) {
          if (e.Code && e.Code.indexOf('MessageNotExist') > -1) {
          // 没有消息，则继续长轮询服务器
          // console.log('Consume Message: no new message, RequestId:%s, Code:%s', e.RequestId, e.Code);
            await sleep(this.options.pullInterval);
          } else {
            this.logger.error('MQPushConsumer occurred an error ' + e.message);
            this.emit('error', e);
            await sleep(this.options.pullTimeDelayMillsWhenException);
          }
        }
      }
    })();
  }

  async executePullRequestImmediately(consumer, topic) {
    // flow control
    const processQueue = this.processQueue.get(topic);
    if (processQueue.length > this.options.pullThresholdForQueue) {
      await sleep(this.options.pullTimeDelayMillsWhenFlowControl);
      return;
    }

    await consumer.consumeMessage(this.options.pullBatchSize, this.options.consumerTimeoutMillisWhenSuspend).then(res => {
      if (res.code === 200) {
        res.body.forEach(message => {
          const { MessageId, MessageBody, MessageKey, ReceiptHandle } = message;
          processQueue.push({
            msgId: MessageId,
            body: MessageBody,
            key: MessageKey,
            ReceiptHandle,
          });
        });
        this.emit(`topic_${topic}_changed`);
        return;
      }
    });
  }

  subscribe(topic, subExpression, handler) {
    if (arguments.length === 2) {
      handler = subExpression;
      subExpression = null;
    }
    assert(is.asyncFunction(handler), '[MQPushConsumer] handler should be a asyncFunction');
    assert(!this.subscriptions.has(topic), `[MQPushConsumer] ONLY one handler allowed for topic=${topic}`);

    this.processQueue.set(topic, []);
    // TODO 是否支持多tag ,tagA||tagB
    let tagsSet = subExpression;
    if (is.nullOrUndefined(subExpression) || subExpression === '*' || subExpression === '') {
      tagsSet = '*';
    }

    // 拉取消息
    const consumer = this._mqClient.getConsumer(this.instanceId, topic, this.consumerGroup, tagsSet);
    this.subscriptions.set(topic, {
      handler,
      consumer,
      tagsSet,
    });

    (async () => {
      try {
        this._pullMessage(consumer, topic);
        // 消息消费循环
        while (this.subscriptions.has(topic)) {
          await this._consumeMessageLoop(topic);
        }

      } catch (err) {
        this._handleError(err);
      }
    })();

    this.logger.info('[MQPushConsumer] cancel subscribe for topic=%s, subExpression=%s', topic, subExpression);
  }


  _handleError(err) {
    err.message = 'MQPushConsumer occurred an error ' + err.message;
    this.emit('error', err);
  }

  async _consumeMessageLoop(topic) {
    const mqList = this.processQueue.get(topic);
    let hasMsg = false;

    while (mqList.length) {
      hasMsg = true;
      let msgs;
      if (this.options.parallelConsumeLimit > mqList.length) {
        msgs = mqList.splice(0, mqList.length);
      } else {
        msgs = mqList.splice(0, this.options.parallelConsumeLimit);
      }
      // 并发消费任务
      const consumeTasks = [];
      for (const msg of msgs) {
        const { handler, consumer } = this.subscriptions.get(topic);
        consumeTasks.push(this.consumeSingleMsg(handler, msg, consumer));
      }
      // 必须全部成功
      try {
        await Promise.all(consumeTasks);
      } catch (err) {
        continue;
      }
    }

    if (!hasMsg) {
      const changedEvent = `topic_${topic}_changed`;
      this.logger.debug(`[MQPushConsumer] waiting event for ${changedEvent}`);
      return await this.await(changedEvent);
    }
  }

  async consumeSingleMsg(handler, msg, consumer) {
    const ReceiptHandle = msg.ReceiptHandle;
    delete msg.ReceiptHandle;
    await handler(msg);
    return await consumer.ackMessage([ ReceiptHandle ]);
  }
}

module.exports = MQPushConsumer;
