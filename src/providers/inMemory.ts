import { EachMessagePayload, CompressionTypes, RecordBatchEntry, KafkaMessage } from 'kafkajs';
import {
    ConsumerDefinition,
    KafkaConfig,
    KafkaConsumerMessage,
    KafkaDlqConsumerMessage,
    KafkaProducerMessage,
    KafkaMessageBusProvider,
} from '../interfaces';
import { v4 as uuidv4 } from 'uuid';
import { extractMetadataFromHeaders } from '../utils';
import { emitLogEvent, logLevels } from '../services/event-registry.service';
import { Middleware, Pipeline } from '../middleware';
import {
    DLQ_SUFFIX
} from '../constants';

const heartbeat: () => Promise<void> = async () => {
    emitLogEvent(logLevels.info, 'KafkaMessageBus: simulating heartbeat');
    Promise.resolve();
};

/* istanbul ignore next */
const pause: () => () => void = () => () => {
    emitLogEvent(logLevels.info, 'KafkaMessageBus: simulating pause');
};

class InMemoryProvider implements KafkaMessageBusProvider {
    consumerDefinitions?: ConsumerDefinition[];
    middlewares?: Middleware<EachMessagePayload>[];
    clientId: string;
    clientPrefix: string;

    constructor({
        config,
        consumerDefinitions,
        middlewares,
    }: {
        config: KafkaConfig;
        consumerDefinitions?: ConsumerDefinition[];
        middlewares?: Middleware<EachMessagePayload>[];
    }) {

        this.middlewares = middlewares;
        this.consumerDefinitions = consumerDefinitions;
        this.clientId = config.clientId;
        this.clientPrefix = `${config.clientId}_`;
        this.consumerHandler = this.consumerHandler.bind(this);
        this.dlqConsumerHandler = this.dlqConsumerHandler.bind(this);
    }

    getConsumerDefinitionByTopic(topic: string): ConsumerDefinition | undefined {
        return this.consumerDefinitions?.find(({ topic: topicName }) => topicName === topic);
    }

    getDlqTopicName(topic: string) {
        return `${this.clientPrefix}${topic}${DLQ_SUFFIX}`;
    }

    getTopicNameFromDlqTopicName(dlqTopic: string): string {
        if (dlqTopic.startsWith(this.clientPrefix) && dlqTopic.endsWith(DLQ_SUFFIX)) {
            return dlqTopic.slice(this.clientPrefix.length, -4); // -4 because "_dlq" is 4 characters
        }
        const errMsg = `KafkaMessageBus In Memory getTopicNameFromDlqTopicName: Invalid DLQ topic name format: ${dlqTopic}.`;
        emitLogEvent(logLevels.error, errMsg);
        throw new Error(errMsg);
    }

    getDlqHandler(consumerDefinition: ConsumerDefinition): ((msg: KafkaDlqConsumerMessage) => Promise<void> | void) | undefined {
        const { deadletterHandler } = consumerDefinition;
        return deadletterHandler;
    }

    async dlqConsumerHandler({ topic, partition, message, heartbeat }: EachMessagePayload) {
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        emitLogEvent(logLevels.info, `KafkaMessageBus In Memory dlqConsumerHandler - ${prefix} ${message.key}#${message.value}`);
        const originalTopic = this.getTopicNameFromDlqTopicName(topic);
        const consumerDefinition = this.getConsumerDefinitionByTopic(originalTopic);
        if (!consumerDefinition) {
            emitLogEvent(logLevels.info, 'KafkaMessageBus In Memory: simulating message consumer of topic that is not defined');
            return;
        }
        const deadletterHandler = this.getDlqHandler(consumerDefinition);
        if (!deadletterHandler) {
            emitLogEvent(logLevels.info, 'KafkaMessageBus In Memory: simulating dlq message consumer of dlq topic that is not defined');
            return;
        }
        try {
            const { headers } = message;
            const consumerDlqMessage = {
                ...extractMetadataFromHeaders(headers),
                message: JSON.parse(message.value?.toString() || '{}'),
            } as KafkaDlqConsumerMessage;
            await deadletterHandler(consumerDlqMessage);
        } catch (err: unknown) {
            emitLogEvent(
                logLevels.error,
                `KafkaMessageBus In Memory dlqConsumerHandler: failed to consume messagae of topic: ${topic}`
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
        message: KafkaMessage;
    }) {
        emitLogEvent(logLevels.error, `KafkaMessageBus In Memory consumerHandler: failed to consume messagae of topic: ${topic}`);
        const { deadletter } = consumerDefinition;
        if (deadletter) {
            const dlqTopic = this.getDlqTopicName(topic);
            const msgEntry: RecordBatchEntry = {
                key: message.key,
                value: message.value,
                timestamp: new Date().toString(),
                attributes: 0,
                offset: '0',
                headers: {
                    ...message.headers,
                    'error-message': err instanceof Error ? err.message : '',
                    'dlq-topic': dlqTopic,
                }
            };
            const msg: EachMessagePayload = {
                topic: dlqTopic,
                partition: 0,
                message: msgEntry,
                heartbeat,
                pause
            };
            return await this.runWithMiddlewarePipeline(msg, this.dlqConsumerHandler);
        }
        throw err;
    }

    async consumerHandler({ topic, partition, message, heartbeat }: EachMessagePayload) {
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        emitLogEvent(logLevels.info, `KafkaMessageBus In Memory consumerHandler - ${prefix} ${message.key}#${message.value}`);
        const consumerDefinition = this.getConsumerDefinitionByTopic(topic);
        if (!consumerDefinition) {
            emitLogEvent(logLevels.info, 'KafkaMessageBus In Memory: simulating message consumer of topic that is not defined');
            return;
        }
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

    messageEnrichment(message: KafkaProducerMessage): RecordBatchEntry {
        return {
            key: Buffer.from(message.key),
            value: Buffer.from(JSON.stringify(message.value)),
            headers: {
                ...message.headers,
                ...(message.causationId && { 'causation-id': message.causationId }),
                'correlation-id': message.correlationId || uuidv4(),
                'message-type': message.messageType,
                'initiator-id': this.clientId,
            },
            timestamp: new Date().toString(),
            attributes: 0,
            offset: '0',
        };
    }

    async send({ topic, message }: { topic: string; message: KafkaProducerMessage }): Promise<void> {
        const enrichedMessage: RecordBatchEntry = this.messageEnrichment(message);
        try {
            const msg: EachMessagePayload = {
                topic,
                partition: 0,
                message: enrichedMessage,
                heartbeat,
                pause
            };
            await this.runWithMiddlewarePipeline(msg, this.consumerHandler);
        } catch (err: unknown) {
            emitLogEvent(
                logLevels.error,
                `KafkaMessageBus In Memory: failed to send message to topic: ${topic}, message: ${JSON.stringify(enrichedMessage)}`
            );
            throw err;
        }
    }

    async sendBulk({
        topic,
        messages,
    }: {
        topic: string;
        messages: KafkaProducerMessage[];
        compression?: CompressionTypes;
    }): Promise<void> {
        const kafkaMessages: RecordBatchEntry[] = messages.map((message) => this.messageEnrichment(message));
        try {
            for (const enrichedMessage of kafkaMessages) {
                const msg: EachMessagePayload = {
                    topic,
                    partition: 0,
                    message: enrichedMessage,
                    heartbeat,
                    pause
                };
                await this.runWithMiddlewarePipeline(msg, this.consumerHandler);
            }
        } catch (err) {
            emitLogEvent(logLevels.error, `KafkaMessageBus In Memory: failed to send bulk messages to topic: ${topic}`);
            throw err;
        }
    }

    async start() {
        emitLogEvent(logLevels.info, 'KafkaMessageBus In Memory: initiating...');
        emitLogEvent(logLevels.info, 'KafkaMessageBus In Memory: finished initiating');
    }

    async stop() {
        emitLogEvent(logLevels.info, 'KafkaMessageBus In Memory: stopping...');
    }
}

export default InMemoryProvider;
