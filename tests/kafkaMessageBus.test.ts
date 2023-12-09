import { KafkaMessageBus } from "../src/kafkaMessageBus";
import InMemoryProvider from '../src/providers/inMemory';
import { KafkaConfig, KafkaProducerMessage } from '../src/interfaces';
import { Provider, Providers} from "../src/types";
import invalidMiddleware from './middlewares/invalidMiddleware';

describe('KafkaMessageBus', () => {
    let config1: KafkaConfig;
    let config2: KafkaConfig;

    beforeEach(() => {
        config1 = {
            provider: "unsupported" as Provider, // Type assertion to bypass type-checking
            clientId: "testClient",
            brokers: ["broker1"],
            username: "username",
            password: "password",
            mechanism: 'plain'
        };

        config2 = {
            provider: Providers.inMemory,
            clientId: "testClient",
            brokers: ["broker1"],
            username: "username",
            password: "password",
            mechanism: 'plain'
        };
    });
    it('should throw error topic uniqueness validation', async () => {
        const consumerDefinition = {
            topic: 'topicExample',
            handler: jest.fn()
        };

        expect(() => {
            new KafkaMessageBus({
                config: config1,
                consumerDefinitions: [consumerDefinition, consumerDefinition]
            });
        }).toThrow('KafkaMessageBus: there are one or more consumers that defined with the same topic');
    });

    it('should throw an error when provider is unsupported', () => {
        expect(() => new KafkaMessageBus({ config: config1 })).toThrow('provider: unsupported is not supported');
    });

    it('should throw an error when listening to unsupported event', () => {
        const bus = new KafkaMessageBus({ config: config2 });
        expect(() => bus.on('unsupported' as any, jest.fn())).toThrow('Invalid event type unsupported');
    });

    it('should throw an error when listening with non functional callback', () => {
        const bus = new KafkaMessageBus({ config: config2 });
        expect(() => bus.on('log', {})).toThrow('callback argument must be of type function');
    });

    it('should create an instance of KafkaMessageBus with inMemory provider', () => {
        const bus = new KafkaMessageBus({ config: config2 });
        expect(bus).toBeInstanceOf(KafkaMessageBus);
    });

    it('should failed to send message due to invalid middleware', async () => {
        const bus = new KafkaMessageBus({ config: config2, middlewares: [invalidMiddleware] });
        const topic = 'undefinedTopic';
        const message: KafkaProducerMessage = {
            key: "testKey",
            messageType: "testType",
            value: { content: "testContent" }
        };

        await expect(bus.send({ topic, message })).rejects.toThrow('next() called multiple times');
    });

    it('should use InMemoryProvider for specific tests', () => {
        new InMemoryProvider({ config: config2 });
    });
})