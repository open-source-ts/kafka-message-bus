import { KafkaMessageBus } from "../../src/kafkaMessageBus";
import { ConsumerDefinition, KafkaProducerMessage, KafkaConfig } from '../../src/interfaces'
import { Providers } from "../../src/types";
import { v4 as uuidv4 } from 'uuid';
import middleware from '../middleware';

describe('KafkaMessageBus with InMemoryProvider', () => {
    let kafkaMessageBus: KafkaMessageBus;
    let config: KafkaConfig;
    let consumerDefinition: ConsumerDefinition;
    const clientId = "testClient"
    const topicName = "testTopic";

    beforeEach(async () => {
        config = {
            provider: Providers.inMemory,
            clientId,
            brokers: ["broker1"],
            username: "username",
            password: "password",
            mechanism: 'plain'
        };
    });

    describe('KafkaMessageBus with InMemoryProvider - without deadletter', () => {
        beforeEach(async () => {
            
            consumerDefinition = {
                topic: topicName,
                handler: jest.fn()
            };
    
            kafkaMessageBus = new KafkaMessageBus({
                config,
                middlewares: [middleware],
                consumerDefinitions: [consumerDefinition]
            });
            kafkaMessageBus.on('log', ({ message }: { message: string }) => {
                console.log(message);
            });
            await kafkaMessageBus.start();
        });

        afterEach(async () => {
            await kafkaMessageBus.stop();
        });

        it('should send message from in-memory provider to a not defined topic without errors', async () => {
            const topic = 'undefinedTopic';
            const message: KafkaProducerMessage = {
                key: "testKey",
                messageType: "testType",
                value: { content: "testContent" }
            };
    
            await kafkaMessageBus.send({ topic, message });
    
            expect(consumerDefinition.handler).toHaveBeenCalledTimes(0);
        });

        it('should consume messages from in-memory provider when sent to the topic', async () => {
            const topic = topicName;
            const message: KafkaProducerMessage = {
                key: "testKey",
                messageType: "testType",
                value: { content: "testContent" }
            };
    
            await kafkaMessageBus.send({ topic, message });
    
            expect(consumerDefinition.handler).toHaveBeenCalledWith(expect.objectContaining({
                causationId: undefined,
                correlationId: expect.any(String),
                dlqTopic: undefined,
                errorMessage: undefined,
                headers: expect.objectContaining({
                    "correlation-id": expect.any(String),
                    "initiator-id": "testClient",
                    "message-type": "testType"
                }),
                initiatorId: "testClient",
                message: {
                    content: "testContent"
                },
                messageType: "testType"
            }));

        });

        it('should successfully send and consume messages in bulk', async () => {
            const topic = topicName;
            const messages = [
                { key: 'testKey1', messageType: 'testType', value: { content: 'content1' } },
                { key: 'testKey2', messageType: 'testType', value: { content: 'content2' } },
            ];
    
            await kafkaMessageBus.sendBulk({ topic, messages });
    
            expect(consumerDefinition.handler).toHaveBeenCalledTimes(2);
        });        
    });

    describe('KafkaMessageBus with InMemoryProvider - with dlq and dlq handler', () => {
        const errorMessage = "Mocked exception";

        beforeEach(async () => {
            
            consumerDefinition = {
                topic: "testTopic",
                handler: jest.fn().mockImplementation(() => {
                    throw new Error(errorMessage);
                }),
                deadletter: true,
                deadletterHandler: jest.fn()
            };
    
            kafkaMessageBus = new KafkaMessageBus({
                config,
                consumerDefinitions: [consumerDefinition]
            });
            kafkaMessageBus.on('log', ({ message }: { message: string }) => {
                console.log(message);
            });
            await kafkaMessageBus.start();
        });

        afterEach(async () => {
            await kafkaMessageBus.stop();
        })

        it('should send a message to the deadletter topic after consumer fails', async () => {
            const topic = topicName;
            const dlqTopic = `${clientId}_${topicName}_dlq`
            const messageType = "testType";
            const msg = { content: "testContent" };

            const message: KafkaProducerMessage = {
                key: "testKey",
                messageType,
                value: msg,
                causationId: uuidv4(),
            };
            const spy = jest.spyOn(kafkaMessageBus.provider as any, 'dlqConsumerHandler');
            await kafkaMessageBus.send({ topic, message });
            expect(spy).toHaveBeenCalledWith(
                expect.objectContaining({
                    topic: dlqTopic,
                    message: expect.objectContaining({
                        key: Buffer.from(message.key),
                        value: Buffer.from(JSON.stringify(message.value))
                    }),
                }),
                expect.anything()
            );

            expect(consumerDefinition.deadletterHandler).toHaveBeenCalledWith(expect.objectContaining({
                causationId: expect.any(String),
                correlationId: expect.any(String),
                dlqTopic: dlqTopic,
                errorMessage,
                headers: expect.objectContaining({
                    "correlation-id": expect.any(String),
                    "initiator-id": clientId,
                    "message-type": messageType,
                    "dlq-topic": dlqTopic
                }),
                initiatorId: clientId,
                message: msg,
                messageType: messageType
            }));

        });
    });

    describe('KafkaMessageBus with InMemoryProvider - with dlq only', () => {
        const errorMessage = "Mocked exception";

        beforeEach(async () => {
            
            consumerDefinition = {
                topic: "testTopic",
                handler: jest.fn().mockImplementation(() => {
                    throw new Error(errorMessage);
                }),
                deadletter: true,
            };
    
            kafkaMessageBus = new KafkaMessageBus({
                config,
                consumerDefinitions: [consumerDefinition]
            });
            kafkaMessageBus.on('log', ({ message }: { message: string }) => {
                console.log(message);
            });
            await kafkaMessageBus.start();
        });

        afterEach(async () => {
            await kafkaMessageBus.stop();
        })

        it('should send a message to the deadletter topic after consumer fails', async () => {
            const topic = topicName;
            const dlqTopic = `${clientId}_${topicName}_dlq`
            const messageType = "testType";
            const msg = { content: "testContent" };

            const message: KafkaProducerMessage = {
                key: "testKey",
                messageType,
                value: msg,
                causationId: uuidv4(),
            };
            const spy = jest.spyOn(kafkaMessageBus.provider as any, 'dlqConsumerHandler');
            await kafkaMessageBus.send({ topic, message });
            expect(spy).toHaveBeenCalledWith(
                expect.objectContaining({
                    topic: dlqTopic,
                    message: expect.objectContaining({
                        key: Buffer.from(message.key),
                        value: Buffer.from(JSON.stringify(message.value))
                    }),
                }),
                expect.anything()
            );
        });
    });
});
