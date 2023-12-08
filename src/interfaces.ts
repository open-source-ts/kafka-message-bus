import { IHeaders, CompressionTypes } from 'kafkajs';
import { Provider } from './types';

export interface KafkaConfig {
    clientId: string;
    brokers: string[];
    username: string;
    password: string;
    mechanism: 'scram-sha-512' | 'scram-sha-256' | 'plain';
    ssl?: boolean;
    provider: Provider
    createTopics?: boolean;
}

export interface ConsumerDefinition {
    topic: string;
    handler: (msg: KafkaConsumerMessage) => Promise<void> | void;
    deadletter?: boolean;
    deadletterHandler?: (msg: KafkaDlqConsumerMessage) => Promise<void> | void;
}

export interface KafkaProducerMessage {
    key: string;
    messageType: string;
    value: object | undefined;
    correlationId?: string;
    causationId?: string;
    headers?: IHeaders;
}

export interface KafkaConsumerMessage {
    correlationId: string;
    causationId?: string;
    messageType: string;
    initiatorId: string;
    message: object;
    headers?: IHeaders;
}

export interface KafkaDlqConsumerMessage {
    correlationId: string;
    causationId?: string;
    messageType: string;
    initiatorId: string;
    dlqTopic: string;
    errorMessage?: string;
    message: object;
    headers?: IHeaders;
}


export interface KafkaMessageBusProvider {
    start(): Promise<void>;

    stop(): Promise<void>;

    send({ topic, message }: { topic: string; message: KafkaProducerMessage }): Promise<void>;

    sendBulk({
        topic,
        messages,
        compression,
    }: {
        topic: string;
        messages: KafkaProducerMessage[];
        compression?: CompressionTypes;
    }): Promise<void>

}