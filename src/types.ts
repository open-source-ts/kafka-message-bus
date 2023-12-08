export { EachMessagePayload, CompressionTypes } from 'kafkajs';
export const Providers = {
    Kafka: 'Kafka',
    inMemory: 'inMemory',
} as const;

export type Provider = typeof Providers[keyof typeof Providers];
