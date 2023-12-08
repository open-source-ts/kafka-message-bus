import { Message, KafkaConfig, RecordBatchEntry, BrokersFunction, EachMessagePayload, KafkaMessage } from 'kafkajs';
import { transformToBufferOrNull } from '../../src/utils';

const kafkajs: any = jest.createMockFromModule('kafkajs');

const heartbeat: () => Promise<void> = async () => {
    await Promise.resolve()
};

const pause: () => () => void = () => () => {
    console.log('simulating pausing');
};

type SendCallback = (args: { topic: string; messages: Message[] }) => void;

class Producer {
  sendCb: SendCallback;

  constructor({ sendCb }: { sendCb: SendCallback }) {
    this.sendCb = sendCb;
  }

  async connect(): Promise<void> {
    return Promise.resolve();
  }

  async send(args: { topic: string; messages: KafkaMessage[] }): Promise<void> {
    this.sendCb(args);
  }

  async disconnect(): Promise<void> {
    return Promise.resolve();
  }
}

class Admin {
  async connect(): Promise<void> {
    return Promise.resolve();
  }

  async disconnect(): Promise<void> {
    return Promise.resolve();
  }

  async createTopics(): Promise<void> {
    return Promise.resolve();
  }
}

class Consumer {
  groupId: string;
  subscribeCb: (topic: string, consumer: Consumer) => void;
  eachMessage?: (msg: EachMessagePayload) => Promise<void>

  constructor({ groupId, subscribeCb }: { groupId: string; subscribeCb: (topic: string, consumer: Consumer) => void }) {
    this.groupId = groupId;
    this.subscribeCb = subscribeCb;
  }

  getGroupId(): string {
    return this.groupId;
  }

  async connect(): Promise<void> {
    return Promise.resolve();
  }

  async subscribe({ topics }: { topics: string [] }): Promise<void> {
    topics.forEach((topic: string) => {
        this.subscribeCb(topic, this);
    }); 
  }

  async run({ eachMessage }: { eachMessage: (msg: EachMessagePayload) => Promise<void> }): Promise<void> {
    this.eachMessage = eachMessage;
  }

  async disconnect(): Promise<void> {
    return Promise.resolve();
  }
}

kafkajs.Kafka = class Kafka {
  brokers: string[] | BrokersFunction;
  clientId:  string | undefined;
  topics: { [key: string]: { [key: string]: Consumer[] } };

  constructor(config: KafkaConfig) {
    this.brokers = config.brokers;
    this.clientId = config.clientId;
    this.topics = {};
  }

  _subscribeCb(topic: string, consumer: Consumer): void {
    if (!this.topics[topic]) {
      this.topics[topic] = {};
    }
    if (!this.topics[topic][consumer.getGroupId()]) {
      this.topics[topic][consumer.getGroupId()] = [];
    }

    this.topics[topic][consumer.getGroupId()].push(consumer);
  }

  _sendCb({ topic, messages }: { topic: string; messages: Message[] }): void {
    if (!this.topics[topic]) {
      return; // No consumer subscribed to this topic
    }

    messages.forEach((message) => {
      Object.values(this.topics[topic]).forEach((consumers) => {
        const consumerToGetMessage = Math.floor(Math.random() * consumers.length);
        if (consumers[consumerToGetMessage].eachMessage) {
            const msgEntry: RecordBatchEntry = {
                key: transformToBufferOrNull(message.key),
                value: transformToBufferOrNull(message.value),
                headers: message.headers || {},
                timestamp: new Date().toString(),
                attributes: 0,
                offset: '0',
            };
            consumers[consumerToGetMessage].eachMessage!({
                topic,
                partition: 0,
                message: msgEntry,
                heartbeat,
                pause
            });
        }
      });
    });
  }

  admin(): Admin {
    return new Admin();
  }

  producer(): Producer {
    return new Producer({
      sendCb: this._sendCb.bind(this),
    });
  }

  consumer({ groupId }: { groupId: string }): Consumer {
    return new Consumer({
      groupId,
      subscribeCb: this._subscribeCb.bind(this),
    });
  }
};

export = kafkajs;
