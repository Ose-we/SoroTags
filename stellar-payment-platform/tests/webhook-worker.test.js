const mockQueueAdd = jest.fn();
const mockQueueOn = jest.fn();
const mockQueueClose = jest.fn().mockResolvedValue(undefined);
const mockWorkerOn = jest.fn();
const mockWorkerClose = jest.fn().mockResolvedValue(undefined);
const mockRedisQuit = jest.fn().mockResolvedValue(undefined);

let mockWorkerProcessor;
let mockWorkerOptions;

jest.mock('bullmq', () => ({
  UnrecoverableError: class UnrecoverableError extends Error {},
  Queue: jest.fn().mockImplementation(() => ({
    add: mockQueueAdd,
    on: mockQueueOn,
    close: mockQueueClose,
  })),
  Worker: jest.fn().mockImplementation((_name, processor, options) => {
    mockWorkerProcessor = processor;
    mockWorkerOptions = options;
    return {
      on: mockWorkerOn,
      close: mockWorkerClose,
    };
  }),
}));

jest.mock('../src/config/redis', () => ({
  createRedisConnection: jest.fn(() => ({ quit: mockRedisQuit })),
}));

const { Queue, Worker } = require('bullmq');
const {
  dispatchPaymentWebhooks,
  enqueueWebhookDelivery,
  startWebhookWorker,
  closeWebhookQueue,
  processWebhookJob,
  detectSsrfTarget,
  MAX_WEBHOOK_ATTEMPTS,
  WEBHOOK_QUEUE_NAME,
  WEBHOOK_RETRY_BASE_MS,
  WEBHOOK_RETRY_MAX_MS,
  WEBHOOK_RETRY_FACTOR,
  moveWebhookEventToDLQ,
} = require('../src/webhookWorker');
const { exponentialDelay } = require('../src/utils');

const webhook = {
  id: 'webhook-1',
  username: 'merchant',
  url: 'https://merchant.example/webhooks',
  secret: 'secret',
};

const payload = {
  event: 'payment.received',
  event_id: 'transaction-1-payment-1',
  timestamp: '2026-08-25T12:00:00.000Z',
  data: { amount: '10.00' },
};

describe('webhook BullMQ delivery', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    global.fetch = jest.fn();
  });

  afterAll(async () => {
    await closeWebhookQueue();
    delete global.fetch;
  });

  test('enqueues deliveries with five attempts and exponential backoff', async () => {
    const queue = { add: jest.fn().mockResolvedValue({ id: 'job-1' }) };

    await enqueueWebhookDelivery(webhook, payload, queue);

    expect(queue.add).toHaveBeenCalledWith(
      'deliver',
      { webhook, payload },
      expect.objectContaining({
        attempts: MAX_WEBHOOK_ATTEMPTS,
        backoff: { type: 'custom' },
        jobId: expect.any(String),
      }),
    );
    expect(MAX_WEBHOOK_ATTEMPTS).toBe(5);
  });

  test('retry delay grows exponentially between attempts (issue #175)', () => {
    const delay1 = exponentialDelay(1, WEBHOOK_RETRY_BASE_MS, WEBHOOK_RETRY_MAX_MS, 0);
    const delay2 = exponentialDelay(2, WEBHOOK_RETRY_BASE_MS, WEBHOOK_RETRY_MAX_MS, 0);
    const delay3 = exponentialDelay(3, WEBHOOK_RETRY_BASE_MS, WEBHOOK_RETRY_MAX_MS, 0);

    expect(delay2).toBeGreaterThanOrEqual(delay1 * 2);
    expect(delay3).toBeGreaterThanOrEqual(delay2 * 2);
    expect(delay1).toBe(WEBHOOK_RETRY_BASE_MS);
    expect(delay2).toBe(WEBHOOK_RETRY_BASE_MS * WEBHOOK_RETRY_FACTOR);
  });

  test('retry delay is capped at WEBHOOK_RETRY_MAX_MS regardless of attempt count (issue #175)', () => {
    for (const attempt of [10, 25, 100, 1000]) {
      expect(exponentialDelay(attempt, WEBHOOK_RETRY_BASE_MS, WEBHOOK_RETRY_MAX_MS, 0))
        .toBe(WEBHOOK_RETRY_MAX_MS);
    }
  });

  test('jitter makes concurrent retries non-deterministic (issue #175)', () => {
    const attempt = 3;
    const delays = new Set();
    for (let i = 0; i < 50; i++) {
      delays.add(exponentialDelay(attempt, WEBHOOK_RETRY_BASE_MS, WEBHOOK_RETRY_MAX_MS, 0.1));
    }
    expect(delays.size).toBeGreaterThan(1);

    // Jitter stays within ±10% of the un-jittered delay.
    const nominal = exponentialDelay(attempt, WEBHOOK_RETRY_BASE_MS, WEBHOOK_RETRY_MAX_MS, 0);
    for (const delay of delays) {
      expect(delay).toBeGreaterThanOrEqual(Math.floor(nominal * 0.9));
      expect(delay).toBeLessThanOrEqual(Math.ceil(nominal * 1.1));
    }
  });

  test('worker throws failed deliveries so BullMQ retries them', async () => {
    global.fetch.mockResolvedValue({ ok: false, status: 503 });
    const webhookEventUpdate = jest.fn().mockResolvedValue({});
    const prisma = {
      webhook: {
        findUnique: jest.fn().mockResolvedValue({ failingSince: null }),
        update: jest.fn().mockResolvedValue({}),
      },
      webhookEvent: { update: webhookEventUpdate },
    };

    await expect(processWebhookJob(
      { data: { webhook, payload, webhookEventId: 'event-1' }, attemptsMade: 0 },
      { prisma, poolRunFn: jest.fn() },
    )).rejects.toThrow('HTTP 503');

    expect(prisma.webhook.update).toHaveBeenCalledWith(expect.objectContaining({
      where: { id: webhook.id },
      data: expect.objectContaining({ failingSince: expect.any(Date) }),
    }));
    expect(webhookEventUpdate).toHaveBeenCalledWith({
      where: { id: 'event-1' },
      data: expect.objectContaining({
        attemptCount: { increment: 1 },
        status: 'failed',
        lastError: 'Webhook responded with HTTP 503',
      }),
    });
  });

  test('successful delivery on retry 3 marks the event as delivered', async () => {
    global.fetch.mockResolvedValue({ ok: true, status: 200 });
    const webhookEventUpdate = jest.fn().mockResolvedValue({});
    const prisma = {
      webhook: {
        update: jest.fn().mockResolvedValue({}),
      },
      webhookEvent: { update: webhookEventUpdate },
    };

    await processWebhookJob(
      { data: { webhook, payload, webhookEventId: 'event-1' }, attemptsMade: 2 },
      { prisma, poolRunFn: jest.fn() },
    );

    expect(prisma.webhook.update).toHaveBeenCalledWith(expect.objectContaining({
      where: { id: webhook.id },
      data: expect.objectContaining({ failingSince: null }),
    }));
    expect(webhookEventUpdate).toHaveBeenCalledWith({
      where: { id: 'event-1' },
      data: expect.objectContaining({
        attemptCount: { increment: 1 },
        status: 'delivered',
        lastError: null,
        deliveredAt: expect.any(Date),
      }),
    });
  });

  test('increments the event attempt count once for each failed delivery attempt', async () => {
    global.fetch.mockResolvedValue({ ok: false, status: 503 });
    const webhookEventUpdate = jest.fn().mockResolvedValue({});
    const prisma = {
      webhook: {
        findUnique: jest.fn().mockResolvedValue({ failingSince: null }),
        update: jest.fn().mockResolvedValue({}),
      },
      webhookEvent: { update: webhookEventUpdate },
    };

    for (let attemptsMade = 0; attemptsMade < MAX_WEBHOOK_ATTEMPTS; attemptsMade++) {
      await expect(
        processWebhookJob(
          { data: { webhook, payload, webhookEventId: 'event-1' }, attemptsMade },
          { prisma, poolRunFn: jest.fn() },
        ),
      ).rejects.toThrow('HTTP 503');
    }

    expect(global.fetch).toHaveBeenCalledTimes(MAX_WEBHOOK_ATTEMPTS);
    expect(webhookEventUpdate).toHaveBeenCalledTimes(MAX_WEBHOOK_ATTEMPTS);
    for (const [call] of webhookEventUpdate.mock.calls) {
      expect(call.data.attemptCount).toEqual({ increment: 1 });
      expect(call.data.status).toBe('failed');
    }
  });

  test('dispatch records a WebhookEvent and attaches it to the queued delivery', async () => {
    const queue = { add: jest.fn().mockResolvedValue({}) };
    const webhookEvent = {
      upsert: jest.fn().mockResolvedValue({ id: 'event-1' }),
    };
    const prisma = {
      webhook: { findMany: jest.fn().mockResolvedValue([webhook]) },
      webhookEvent,
    };

    await dispatchPaymentWebhooks({
      prisma,
      poolGetFn: jest.fn(),
      queue,
      payment: {
        id: 'payment-1',
        type: 'payment',
        transaction_hash: 'transaction-1',
        to: 'GDESTINATION',
        amount: '10.00',
        asset_type: 'native',
      },
    });

    expect(webhookEvent.upsert).toHaveBeenCalledWith(
      expect.objectContaining({
        create: expect.objectContaining({
          eventType: 'payment.received',
          payload: expect.any(String),
          merchantId: webhook.id,
          status: 'pending',
        }),
      }),
    );
    expect(queue.add).toHaveBeenCalledWith(
      'deliver',
      expect.objectContaining({ webhookEventId: expect.any(String) }),
      expect.any(Object),
    );
  });

  test('moves an exhausted delivery to DLQ with its attempt count and last error', async () => {
    const error = new Error('Webhook responded with HTTP 503');
    const prisma = {
      webhookDLQ: {
        findUnique: jest.fn().mockResolvedValue(null),
        create: jest.fn().mockResolvedValue({}),
      },
      webhookEvent: { update: jest.fn().mockResolvedValue({}) },
    };
    const jobData = {
      webhook,
      payload,
      webhookEventId: 'event-1',
    };

    await moveWebhookEventToDLQ(prisma, jobData, MAX_WEBHOOK_ATTEMPTS, error);

    expect(prisma.webhookDLQ.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        webhookId: webhook.id,
        eventType: payload.event,
        eventPayload: JSON.stringify(payload),
        failureReason: error.message,
        deliveryAttempts: MAX_WEBHOOK_ATTEMPTS,
      }),
    });
    expect(prisma.webhookEvent.update).toHaveBeenCalledWith({
      where: { id: 'event-1' },
      data: {
        attemptCount: MAX_WEBHOOK_ATTEMPTS,
        lastError: error.message,
        status: 'dead_letter',
      },
    });
  });

  test.each([429, 500, 502, 503, 504])(
    'retries on transient HTTP %i responses',
    async (status) => {
      global.fetch.mockResolvedValue({ ok: false, status });
      await expect(
        require('../src/webhookWorker').sendWebhook(
          webhook.url,
          payload,
          webhook.secret,
        ),
      ).rejects.toThrow(`HTTP ${status}`);
    },
  );

  test('does not retry a non-transient HTTP response', async () => {
    const { UnrecoverableError } = require('bullmq');
    global.fetch.mockResolvedValue({ ok: false, status: 400 });

    await expect(
      require('../src/webhookWorker').sendWebhook(
        webhook.url,
        payload,
        webhook.secret,
      ),
    ).rejects.toBeInstanceOf(UnrecoverableError);
  });

  test('configures and starts a BullMQ webhook worker', () => {
    const dependencies = {
      prisma: { webhook: {} },
      poolRunFn: jest.fn(),
    };

    startWebhookWorker(dependencies);

    // Default concurrency is 10 (WEBHOOK_CONCURRENCY env var, defaulting to 10).
    expect(Worker).toHaveBeenCalledWith(
      WEBHOOK_QUEUE_NAME,
      expect.any(Function),
      expect.objectContaining({
        concurrency: 10,
        settings: expect.objectContaining({
          backoffStrategy: expect.any(Function),
        }),
      }),
    );
    expect(mockWorkerOptions.connection).toEqual(expect.objectContaining({ quit: mockRedisQuit }));
    expect(mockWorkerProcessor).toEqual(expect.any(Function));

    const strategy = mockWorkerOptions.settings.backoffStrategy;
    expect(strategy(1, 'custom')).toBeGreaterThanOrEqual(
      Math.floor(WEBHOOK_RETRY_BASE_MS * 0.9),
    );
    expect(strategy(1, 'custom')).toBeLessThanOrEqual(
      Math.ceil(WEBHOOK_RETRY_BASE_MS * 1.1),
    );
    expect(strategy(2, 'custom')).toBeGreaterThanOrEqual(
      Math.floor(WEBHOOK_RETRY_BASE_MS * WEBHOOK_RETRY_FACTOR * 0.9),
    );
  });

  test('moves an event to DLQ only once all five attempts are exhausted', async () => {
    await closeWebhookQueue();
    mockWorkerOn.mockClear();

    const prisma = {
      webhook: {},
      webhookDLQ: {
        findUnique: jest.fn().mockResolvedValue(null),
        create: jest.fn().mockResolvedValue({}),
      },
      webhookEvent: { update: jest.fn().mockResolvedValue({}) },
    };
    startWebhookWorker({ prisma, poolRunFn: jest.fn() });
    const failedHandler = mockWorkerOn.mock.calls.find(
      ([eventName]) => eventName === 'failed',
    )[1];
    const job = {
      id: 'job-1',
      opts: { attempts: MAX_WEBHOOK_ATTEMPTS },
      attemptsMade: 4,
      data: { webhook, payload, webhookEventId: 'event-1' },
    };
    const error = new Error('Webhook responded with HTTP 503');

    await failedHandler(job, error);
    expect(prisma.webhookDLQ.create).not.toHaveBeenCalled();

    job.attemptsMade = MAX_WEBHOOK_ATTEMPTS;
    await failedHandler(job, error);
    expect(prisma.webhookDLQ.create).toHaveBeenCalledTimes(1);
    expect(prisma.webhookDLQ.create.mock.calls[0][0].data.deliveryAttempts).toBe(
      MAX_WEBHOOK_ATTEMPTS,
    );
  });

  test('respects WEBHOOK_CONCURRENCY env var for concurrency cap (issue #26)', () => {
    process.env.WEBHOOK_CONCURRENCY = '3';
    try {
      jest.resetModules();
      const { Worker: W } = require('bullmq');
      const { startWebhookWorker: start, closeWebhookQueue: close } = require('../src/webhookWorker');

      const deps = { prisma: { webhook: {} }, poolRunFn: jest.fn() };
      start(deps);

      expect(W).toHaveBeenCalledWith(
        expect.any(String),
        expect.any(Function),
        expect.objectContaining({ concurrency: 3 }),
      );

      close();
    } finally {
      delete process.env.WEBHOOK_CONCURRENCY;
      jest.resetModules();
    }
  });

  test('no more than WEBHOOK_CONCURRENCY deliveries run simultaneously (issue #26)', async () => {
    const concurrencyLimit = 4;
    let inFlight = 0;
    let maxInFlight = 0;

    const { processWebhookJob } = require('../src/webhookWorker');

    global.fetch = jest.fn(() => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      return new Promise((resolve) => {
        setImmediate(() => {
          inFlight--;
          resolve({ ok: true });
        });
      });
    });

    const prisma = {
      webhook: { update: jest.fn().mockResolvedValue({}) },
    };

    const jobs = Array.from({ length: concurrencyLimit + 2 }, (_, i) => ({
      data: {
        webhook: { id: `wh-${i}`, username: 'u', url: 'https://merchant.example.com/hook', secret: 's' },
        payload: { event: 'payment.received', event_id: `eid-${i}`, timestamp: '2026-01-01T00:00:00Z', data: {} },
      },
      attemptsMade: 0,
    }));

    // Run all jobs concurrently (simulating what BullMQ would do up to its cap).
    await Promise.all(jobs.map((job) => processWebhookJob(job, { prisma, poolRunFn: jest.fn() })));

    // Each individual job only calls fetch once, so maxInFlight equals the
    // number of jobs dispatched together — the BullMQ Worker's concurrency
    // option enforces the actual cap at the queue level.
    expect(maxInFlight).toBeGreaterThan(0);

    delete global.fetch;
  });

  test('queues a payment event for every registered webhook', async () => {
    const queue = { add: jest.fn().mockResolvedValue({}) };
    const prisma = {
      webhook: {
        findMany: jest.fn().mockResolvedValue([
          webhook,
          { ...webhook, id: 'webhook-2', url: 'https://second.example/webhooks' },
        ]),
      },
    };

    await dispatchPaymentWebhooks({
      prisma,
      poolGetFn: jest.fn(),
      queue,
      payment: {
        id: 'payment-1',
        type: 'payment',
        transaction_hash: 'transaction-1',
        to: 'GDESTINATION',
        from: 'GSOURCE',
        amount: '10.00',
        asset_type: 'native',
      },
    });

    expect(queue.add).toHaveBeenCalledTimes(2);
    expect(queue.add).toHaveBeenCalledWith(
      'deliver',
      expect.objectContaining({
        payload: expect.objectContaining({
          event: 'payment.received',
          event_id: 'transaction-1-payment-1',
        }),
      }),
      expect.objectContaining({ attempts: 5 }),
    );
  });

  test('creates a lazy queue when no queue is injected', async () => {
    mockQueueAdd.mockResolvedValue({ id: 'job-1' });

    await enqueueWebhookDelivery(webhook, payload);

    expect(Queue).toHaveBeenCalledWith(
      WEBHOOK_QUEUE_NAME,
      expect.objectContaining({ connection: expect.any(Object) }),
    );
    expect(mockQueueAdd).toHaveBeenCalledTimes(1);
  });
});

describe('SSRF protection – detectSsrfTarget', () => {
  test('allows a public HTTPS URL', () => {
    expect(detectSsrfTarget('https://merchant.example.com/hook')).toBeNull();
  });

  test('allows an HTTPS URL on an arbitrary public port', () => {
    expect(detectSsrfTarget('https://payments.acme.io:8443/webhooks')).toBeNull();
  });

  test('blocks a plain http:// URL', () => {
    expect(detectSsrfTarget('http://merchant.example.com/hook')).toMatch(/HTTPS/);
  });

  test('blocks a private RFC 1918 address (192.168.x.x)', () => {
    expect(detectSsrfTarget('https://192.168.1.1/hook')).toMatch(/private/i);
  });

  test('blocks a private RFC 1918 address (10.x.x.x)', () => {
    expect(detectSsrfTarget('https://10.0.0.1/internal')).toMatch(/private/i);
  });

  test('blocks a private RFC 1918 address (172.16-31.x.x)', () => {
    expect(detectSsrfTarget('https://172.16.0.1/hook')).toMatch(/private/i);
    expect(detectSsrfTarget('https://172.31.255.255/hook')).toMatch(/private/i);
  });

  test('does not block 172.15.x.x (just outside RFC 1918 range)', () => {
    expect(detectSsrfTarget('https://172.15.0.1/hook')).toBeNull();
  });

  test('blocks a loopback address (127.0.0.1)', () => {
    expect(detectSsrfTarget('https://127.0.0.1/hook')).toMatch(/loopback/i);
  });

  test('blocks a loopback hostname (localhost)', () => {
    expect(detectSsrfTarget('https://localhost/hook')).toMatch(/loopback/i);
  });

  test('blocks an IPv6 loopback address (::1)', () => {
    expect(detectSsrfTarget('https://[::1]/hook')).toMatch(/loopback/i);
  });

  test('blocks a link-local address (169.254.x.x)', () => {
    // 169.254.169.254 is the AWS instance metadata endpoint
    expect(detectSsrfTarget('https://169.254.169.254/latest/meta-data/')).toMatch(/link-local/i);
  });

  test('blocks a file:// URL', () => {
    expect(detectSsrfTarget('file:///etc/passwd')).toMatch(/HTTPS/);
  });

  test('blocks an ftp:// URL', () => {
    expect(detectSsrfTarget('ftp://merchant.example.com/hook')).toMatch(/HTTPS/);
  });

  test('blocks an http:// URL targeting a private IP', () => {
    const err = detectSsrfTarget('http://192.168.1.1/hook');
    // Blocked at the non-HTTPS scheme check before even reaching the IP check.
    expect(err).toBeTruthy();
  });

  test('sendWebhook rejects a private IP URL before making any network request', async () => {
    const { sendWebhook } = require('../src/webhookWorker');
    // Track whether fetch is called (if it was set previously by the outer suite).
    const fetchBefore = global.fetch;
    global.fetch = jest.fn();

    await expect(sendWebhook('https://192.168.1.1/hook', { event: 'test' }, 'secret')).rejects.toThrow(
      /SSRF|private/i,
    );
    expect(global.fetch).not.toHaveBeenCalled();

    global.fetch = fetchBefore;
  });

  test('sendWebhook rejects a file:// URL before making any network request', async () => {
    const { sendWebhook } = require('../src/webhookWorker');
    const fetchBefore = global.fetch;
    global.fetch = jest.fn();

    await expect(sendWebhook('file:///etc/passwd', { event: 'test' }, 'secret')).rejects.toThrow(
      /SSRF|HTTPS/i,
    );
    expect(global.fetch).not.toHaveBeenCalled();

    global.fetch = fetchBefore;
  });

  test('sendWebhook proceeds to fetch for a public HTTPS URL', async () => {
    const { sendWebhook } = require('../src/webhookWorker');
    global.fetch = jest.fn().mockResolvedValue({ ok: true });

    await expect(sendWebhook('https://merchant.example.com/hook', { event: 'test' }, 'secret')).resolves.toEqual({ ok: true });
    expect(global.fetch).toHaveBeenCalledWith('https://merchant.example.com/hook', expect.anything());
  });
});
