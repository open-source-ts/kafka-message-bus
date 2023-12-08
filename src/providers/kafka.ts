import { Kafka, Admin, Producer, Consumer, Message, EachMessagePayload, CompressionTypes, SASLOptions } from 'kafkajs';
import {
    ConsumerDefinition,
    KafkaConfig,
    KafkaConsumerMessage,
    KafkaDlqConsumerMessage,
    KafkaMessageBusProvider,
    KafkaProducerMessage,
} from '../interfaces';
import { v4 as uuidv4 } from 'uuid';
import { extractMetadataFromHeaders } from '../utils';
import { emitLogEvent, logLevels } from '../services/event-registry.service';
import { Middleware, Pipeline } from '../middleware';
import {
    PARTITIONS_CONSUMED_CONCURRENTLY,
    CONSUMER_SESSION_TIMEOUT,
    DLQ_CONSUMER_SESSION_TIMEOUT,
    DLQ_SUFFIX
} from '../constants';

class KafkaProvider implements KafkaMessageBusProvider {
    admin: Admin;
    producer: Producer;
    consumer: Consumer;
    dlqConsumer: Consumer;
    consumerDefinitions?: ConsumerDefinition[];
    middlewares?: Middleware<EachMessagePayload>[];
    clientId: string;
    clientPrefix: string;
    createTopics?: boolean;

    constructor({
        config,
        consumerDefinitions,
        middlewares,
    }: {
        config: KafkaConfig;
        consumerDefinitions?: ConsumerDefinition[];
        middlewares?: Middleware<EachMessagePayload>[];
    }) {
        const kafka = new Kafka({
            clientId: config.clientId,
            brokers: config.brokers,
            ssl: config.ssl ?? true,
            sasl: {
                mechanism: config.mechanism,
                username: config.username,
                password: config.password,
        
            } as SASLOptions, 
        });
        this.middlewares = middlewares;
        this.producer = kafka.producer();
        this.admin = kafka.admin();
        this.consumer = kafka.consumer({ groupId: config.clientId, sessionTimeout: CONSUMER_SESSION_TIMEOUT });
        this.dlqConsumer = kafka.consumer({ groupId: `${config.clientId}${DLQ_SUFFIX}`, sessionTimeout: DLQ_CONSUMER_SESSION_TIMEOUT });
        this.consumerDefinitions = consumerDefinitions;
        this.clientId = config.clientId;
        this.createTopics = config.createTopics;
        this.clientPrefix = `${config.clientId}_`;
        this.consumerHandler = this.consumerHandler.bind(this);
        this.dlqConsumerHandler = this.dlqConsumerHandler.bind(this);
    }

    getConsumerDefinitionByTopic(topic: string): ConsumerDefinition {
        const consumerDefinition = this.consumerDefinitions?.find(({ topic: topicName }) => topicName === topic);
        if (!consumerDefinition) {
            const errMessage = `KafkaMessageBus getConsumerDefinitionByTopic: consumer definition not found for topic: ${topic}`;
            emitLogEvent(logLevels.error, errMessage);
            throw new Error(errMessage);
        }
        return consumerDefinition;
    }

    getDlqTopicName(topic: string) {
        return `${this.clientPrefix}${topic}${DLQ_SUFFIX}`;
    }

    getTopicNameFromDlqTopicName(dlqTopic: string): string {
        if (dlqTopic.startsWith(this.clientPrefix) && dlqTopic.endsWith(DLQ_SUFFIX)) {
            return dlqTopic.slice(this.clientPrefix.length, -4); // -4 because "_dlq" is 4 characters
        }
        const errMsg = `KafkaMessageBus getTopicNameFromDlqTopicName: Invalid DLQ topic name format: ${dlqTopic}.`;
        emitLogEvent(logLevels.error, errMsg);
        throw new Error(errMsg);
    }

    getDlqHandler(consumerDefinition: ConsumerDefinition): (msg: KafkaDlqConsumerMessage) => Promise<void> | void {
        const { topic, deadletterHandler } = consumerDefinition;
        if (!deadletterHandler) {
            const errMessage = `KafkaMessageBus getDlqHandler: deadletter handler is not defined for consumer definition of topic: ${topic}`;
            emitLogEvent(logLevels.error, errMessage);
            throw new Error(errMessage);
        }
        return deadletterHandler;
    }

    async dlqConsumerHandler({ topic, partition, message, heartbeat }: EachMessagePayload) {
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        emitLogEvent(logLevels.info, `KafkaMessageBus dlqConsumerHandler - ${prefix} ${message.key}#${message.value}`);
        const originalTopic = this.getTopicNameFromDlqTopicName(topic);
        const consumerDefinition = this.getConsumerDefinitionByTopic(originalTopic);
        const deadletterHandler = this.getDlqHandler(consumerDefinition);
        try {
            const { headers } = message;
            const consumerDlqMessage = {
                ...extractMetadataFromHeaders(headers),
                message: JSON.parse(message?.value?.toString() || '{}'),
            } as KafkaDlqConsumerMessage;
            await deadletterHandler(consumerDlqMessage);
        } catch (err: unknown) {
            emitLogEvent(
                logLevels.error,
                `KafkaMessageBus dlqConsumerHandler: failed to consume messagae of topic: ${topic}`
            );
            throw err;
        } finally {
            await heartbeat();
        }
    }

    async consumerFailureHandler({
        err,
        topic,
        consumerDefinition,
        message,
    }: {
        err: unknown;
        topic: string;
        consumerDefinition: ConsumerDefinition;
        message: Message;
    }) {
        emitLogEvent(logLevels.error, `KafkaMessageBus consumerHandler: failed to consume messagae of topic: ${topic}`);
        const { deadletter } = consumerDefinition;
        if (deadletter) {
            const dlqTopic = this.getDlqTopicName(topic);
            message.headers = {
                ...message.headers,
                'error-message': err instanceof Error ? err.message : '',
                'dlq-topic': dlqTopic,
            };
            return await this.producer.send({
                topic: dlqTopic,
                messages: [message],
            });
        }
        throw err;
    }

    async consumerHandler({ topic, partition, message, heartbeat }: EachMessagePayload) {
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        emitLogEvent(logLevels.info, `KafkaMessageBus consumerHandler - ${prefix} ${message.key}#${message.value}`);
        const consumerDefinition = this.getConsumerDefinitionByTopic(topic);
        const { handler } = consumerDefinition;
        try {
            const { headers } = message;
            const consumerMessage = {
                ...extractMetadataFromHeaders(headers),
                message: JSON.parse(message.value?.toString() || '{}'),
            } as KafkaConsumerMessage;
            await handler(consumerMessage);
        } catch (err: unknown) {
            await this.consumerFailureHandler({ err, message, consumerDefinition, topic });
        } finally {
            await heartbeat();
        }
    }

    async runWithMiddlewarePipeline(msg: EachMessagePayload, handler: (msg: EachMessagePayload) => Promise<void>) {
        const pipeline = Pipeline<EachMessagePayload>();
        if (this.middlewares) {
            pipeline.push(...this.middlewares);
        }
        pipeline.push(handler);
        await pipeline.execute(msg);
    }

    async startConsumer() {
        await this.consumer.connect();
        await this.consumer.subscribe({ topics: this.consumerDefinitions?.map(({ topic }) => topic) || [] });
        await this.consumer.run({
            partitionsConsumedConcurrently: PARTITIONS_CONSUMED_CONCURRENTLY,
            eachMessage: (msg: EachMessagePayload) => this.runWithMiddlewarePipeline(msg, this.consumerHandler),
        });
    }

    async startDlqConsumer() {
        await this.dlqConsumer.connect();
        const deadletterTopics = this.consumerDefinitions
        ?.filter(({ deadletter, deadletterHandler }) => deadletter && deadletterHandler)
        ?.map(({ topic }) => this.getDlqTopicName(topic)) || [];
        await this.dlqConsumer.subscribe({
            topics: deadletterTopics
        });
        await this.dlqConsumer.run({
            partitionsConsumedConcurrently: PARTITIONS_CONSUMED_CONCURRENTLY,
            eachMessage: (msg: EachMessagePayload) => this.runWithMiddlewarePipeline(msg, this.dlqConsumerHandler),
        });
    }

    async startConsumers() {
        await this.startConsumer();
        await this.startDlqConsumer();
    }

    registerTopics = async () => {
        const topics = this.consumerDefinitions?.map(({ topic }) => ({
            topic,
        })) || [];
        const deadletterTopics = this.consumerDefinitions
            ?.filter(({ deadletter }) => deadletter)
            ?.map(({ topic }) => ({
                topic: this.getDlqTopicName(topic),
            })) || [];
        const registerTopics = {
            waitForLeaders: true,
            topics: [...deadletterTopics, ...topics],
        };

        await this.admin.createTopics(registerTopics);
    };

    messageEnrichment(message: KafkaProducerMessage): Message {
        return {
            key: message.key,
            value: JSON.stringify(message.value),
            headers: {
                ...message.headers,
                ...(message.causationId && { 'causation-id': message.causationId }),
                'correlation-id': message.correlationId || uuidv4(),
                'message-type': message.messageType,
                'initiator-id': this.clientId,
            },
        };
    }

    async send({ topic, message }: { topic: string; message: KafkaProducerMessage }): Promise<void> {
        const kafkaMessage: Message = this.messageEnrichment(message);
        try {
            await this.producer.send({
                topic: topic,
                messages: [kafkaMessage],
            });
        } catch (err: unknown) {
            emitLogEvent(
                logLevels.error,
                `KafkaMessageBus: failed to send message to topic: ${topic}, message: ${JSON.stringify(kafkaMessage)}`
            );
            throw err;
        }
    }

    async sendBulk({
        topic,
        messages,
        compression,
    }: {
        topic: string;
        messages: KafkaProducerMessage[];
        compression?: CompressionTypes;
    }): Promise<void> {
        const kafkaMessages = messages.map((message) => this.messageEnrichment(message));
        try {
            await this.producer.send({
                topic: topic,
                messages: kafkaMessages,
                compression,
            });
        } catch (err) {
            emitLogEvent(logLevels.error, `KafkaMessageBus: failed to send bulk messages to topic: ${topic}`);
            throw err;
        }
    }

    async start() {
        emitLogEvent(logLevels.info, 'KafkaMessageBus: initiating...');
        await this.admin.connect();
        if (this.createTopics) {
            await this.registerTopics();
        }
        await this.startConsumers();
        await this.producer.connect();
        emitLogEvent(logLevels.info, 'KafkaMessageBus: finished initiating');
    }

    async stop() {
        await this.producer.disconnect();
        await this.consumer.disconnect();
        await this.admin.disconnect();
    }
}

export default KafkaProvider;