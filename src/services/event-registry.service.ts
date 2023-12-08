import EventEmitter from 'events';

const eventEmitter = new EventEmitter({ captureRejections: false });

const supportedEvents = {
    log: 'log',
} as const;

const logLevels = {
    info: 'info',
    warn: 'warn',
    error: 'error',
} as const;

export type LogLevel = typeof logLevels[keyof typeof logLevels];

/**
 * Emit event
 * @param {string} event
 * @param params
 */
function emitEvent(event: any, ...params: any) {
    eventEmitter.emit(event, ...params);
}

/**
 * Add event listener
 * @param {string} event
 * @param {callback} callback
 */
function addEventListener(event: any, callback: any) {
    eventEmitter.on(event, callback);
}

/**
 * Emit Log Event
 * @param logLevel
 * @param message
 */
function emitLogEvent(logLevel: LogLevel, message: string) {
    emitEvent(supportedEvents.log, { level: logLevel, message });
}

export { addEventListener, supportedEvents, logLevels, emitLogEvent };
