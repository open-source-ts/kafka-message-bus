import { IHeaders } from 'kafkajs';

// The input type can have values which are Buffers or other types.
type InputObjectType =
    | {
          [key: string]: Buffer | any;
      }
    | undefined;

// The return type can have values which are strings or other types.
type TransformedObjectType = {
    [key: string]: string | object;
} | undefined;

export function transformBufferObject(obj: InputObjectType): TransformedObjectType {
    const transformedObject: TransformedObjectType = {};
    if (!obj) {
        return obj;
    }
    for (const key in obj) {
        if (obj[key] instanceof Buffer) {
            // Convert buffer to utf8 string
            const str = obj[key].toString('utf8');

            try {
                // Try to parse it as a JSON object
                transformedObject[key] = JSON.parse(str);
            } catch (e) {
                // If it's not a JSON string, just assign the string value
                transformedObject[key] = str;
            }
        } else {
            transformedObject[key] = obj[key];
        }
    }

    return transformedObject;
}

export const transformToBufferOrNull = (input: string | Buffer | null | undefined): Buffer | null => {
    if (typeof input === 'string') {
        return Buffer.from(input);
    }
    
    if (input instanceof Buffer) {
        return input;
    }
    
    return null;
};

export function extractMetadataFromHeaders(headers: IHeaders | undefined) {
    const headersObj = transformBufferObject(headers);
    return {
        correlationId: headersObj?.['correlation-id'],
        causationId: headersObj?.['causation-id'],
        messageType: headersObj?.['message-type'],
        initiatorId: headersObj?.['initiator-id'],
        dlqTopic: headersObj?.['dlq-topic'],
        errorMessage: headersObj?.['error-message'],
        headers: headersObj,
    };
}
