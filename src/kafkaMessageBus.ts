import { CompressionTypes, EachMessagePayload } from 'kafkajs';
import { Middleware } from './middleware';
import { Providers } from './types';
import { ConsumerDefinition, KafkaConfig, KafkaMessageBusProvider, KafkaProducerMessage } from './interfaces';
import KafkaProvider from './providers/kafka';
import InMemoryProvider from './providers/inMemory';
import { supportedEvents, addEventListener } from './services/event-registry.service';

export class KafkaMessageBus {
    provider: KafkaMessageBusProvider;

    constructor({ config, consumerDefinitions, middlewares }: { config: KafkaConfig, consumerDefinitions?: ConsumerDefinition[], middlewares?: Middleware<EachMessagePayload>[] }) {
        let provider: KafkaMessageBusProvider | undefined;

        const hasDuplicateTopics = consumerDefinitions 
            ? consumerDefinitions.some((obj, idx) => consumerDefinitions.slice(0, idx).filter(o => o.topic === obj.topic).length > 0)
            : false;
        
        if (hasDuplicateTopics) {
            throw new Error('KafkaMessageBus: there are one or more consumers that defined with the same topic');
        }

        if (config.provider === Providers.Kafka) {
            provider = new KafkaProvider({
                config,
                consumerDefinitions,
                middlewares,
            });
        } else if (config.provider === Providers.inMemory) {
            provider = new InMemoryProvider({
                config,
                consumerDefinitions,
                middlewares,
            });
        } else {
            throw new Error(`provider: ${config.provider} is not supported`)
        }
        this.provider = provider;
    }

    async stop(): Promise<void> {
        await this.provider.stop();
    }

    async start(): Promise<void> {
        await this.provider.start();
    }

    async send({ topic, message }: { topic: string; message: KafkaProducerMessage }): Promise<void> {
        await this.provider.send({ topic, message });
    }

    on(eventType: string, callback: any): void {
        if (!Object.prototype.hasOwnProperty.call(supportedEvents, eventType)) {
            throw new Error(`Invalid event type ${eventType}`);
        }
        if (typeof callback !== 'function') {
            throw new Error('callback argument must be of type function');
        }
        addEventListener(eventType, callback);
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
        await this.provider.sendBulk({ topic, messages, compression });
    }
}