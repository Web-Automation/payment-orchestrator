import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { env } from '../config/env.js';

/**
 * Event types published at every stage of the orchestration flow.
 */
export const EventType = Object.freeze({
  ATTEMPT_STARTED: 'ATTEMPT_STARTED',
  ATTEMPT_FAILED: 'ATTEMPT_FAILED',
  REDIRECTED: 'REDIRECTED', // gateway accepted the session; user handed a redirect URL (not yet settled)
  CASCADED: 'CASCADED',
  VERIFIED_RECOVERED: 'VERIFIED_RECOVERED', // pre-failover status check found the "timed out" attempt actually succeeded
  WEBHOOK_RECEIVED: 'WEBHOOK_RECEIVED', // async settlement confirmation arrived from a gateway
  RECONCILED_REFUND: 'RECONCILED_REFUND', // double-capture detected; auto-refund fired on the late-settling gateway
  FINAL_OUTCOME: 'FINAL_OUTCOME',
});

/**
 * In-memory mock publisher used when SQS credentials/config aren't
 * available (e.g. local dev without localstack, or unit tests). Keeps a
 * bounded in-memory log so tests/dashboards can introspect what was
 * published without needing real infrastructure.
 */
export class MockEventPublisher {
  constructor(logger) {
    this.logger = logger;
    this.published = [];
  }

  async publish(event) {
    this.published.push(event);
    this.logger?.info({ event }, 'event published (mock queue)');
    return { messageId: `mock-${this.published.length}` };
  }

  getPublished() {
    return this.published;
  }

  clear() {
    this.published = [];
  }
}

/** Real SQS-backed publisher. */
export class SqsEventPublisher {
  constructor(logger, opts = {}) {
    this.logger = logger;
    this.queueUrl = opts.queueUrl ?? env.events.sqsQueueUrl;
    this.client = new SQSClient({
      region: opts.region ?? env.events.region,
      ...(opts.endpoint ?? env.events.sqsEndpoint
        ? { endpoint: opts.endpoint ?? env.events.sqsEndpoint }
        : {}),
    });
  }

  async publish(event) {
    const command = new SendMessageCommand({
      QueueUrl: this.queueUrl,
      MessageBody: JSON.stringify(event),
    });
    const result = await this.client.send(command);
    this.logger?.info({ eventType: event.type, messageId: result.MessageId }, 'event published (sqs)');
    return { messageId: result.MessageId };
  }
}

/** Factory: picks SQS or mock based on env.events.driver. */
export function createEventPublisher(logger) {
  if (env.events.driver === 'sqs' && env.events.sqsQueueUrl) {
    return new SqsEventPublisher(logger);
  }
  return new MockEventPublisher(logger);
}

/** Helper to build a well-shaped event envelope. */
export function buildEvent(type, payload) {
  return {
    type,
    timestamp: new Date().toISOString(),
    ...payload,
  };
}
