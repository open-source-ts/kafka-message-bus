import * as os from 'os';

export const PARTITIONS_CONSUMED_CONCURRENTLY = os.cpus().length || 1;
export const CONSUMER_SESSION_TIMEOUT = 60000; // 1 minute
export const DLQ_CONSUMER_SESSION_TIMEOUT = 30000; // 30 sec
export const DLQ_SUFFIX = '_dlq';