import { EachMessagePayload } from 'kafkajs';
import { Middleware } from '../../src';

const middleware: Middleware<EachMessagePayload> = async (_eachMessage, next) => {
    await next();
    await next()
};

export default middleware