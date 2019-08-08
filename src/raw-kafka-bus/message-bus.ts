import {Logger} from 'msv-logger';
import * as Kafka from 'no-kafka';

export type Unsubscriber = () => Promise<void>;

export class MessageBus {
    public logger: Logger;

    private consumers: Kafka.GroupConsumer[];
    private kafkaServers: string;
    private logFunction: (type: 'INFO' | 'DEBUG' | 'WARN' | 'ERROR', ...rest: any[]) => void;
    private producer: Kafka.Producer;

    constructor({
        kafkaServers,
        logger,
    }: {
        kafkaServers: string,
        logger: Logger;
    }) {
        this.consumers = [];
        this.kafkaServers = kafkaServers;
        this.logger = logger;

        const logMethods = {
            INFO: this.logger.log,
            DEBUG: this.logger.debug,
            WARN: this.logger.warn,
            ERROR: this.logger.error,
        };
        this.logFunction = (msgType, ...args) => {
            (logMethods[msgType] || this.logger.warn)(...args);
        };
    }

    public async init(): Promise<void> {
        this.producer = new Kafka.Producer({
            connectionString: this.kafkaServers,
            /*partitioner: function(name, list, message) {
                return Math.floor(Math.random() * list.length); // send each message to random partition
            }*/
            logger: {
                logLevel: 4,
                logFunction: this.logFunction,
            },
        });
        await this.producer.init();
    }

    /**
     * Send message(s) to topic
     */
    public async send(topic: string, messages: {} | Array<{}>) {
        if (!Array.isArray(messages)) {
            messages = [messages];
        }
        if (this.logger) {
            this.logger.debug(`Sending ${(messages as Array<{}>).length} messages to topic "${topic}"`);
        }
        await this.producer.send((messages as Array<{}>).map((m) => ({
            topic,
            message: {
                key: '0',
                value: JSON.stringify(m),
            },
        })));
    }

    /**
     * Start to consume messages from topic
     */
    public async consume(
        topic: string,
        {groupId, concurrency}: {
            groupId: string;
            concurrency: number;
        },
        handler: (data: any) => Promise<void>,
    ): Promise<Unsubscriber> {
        // await this._createTopics([topic]);
        // this.log(`Starting consume from topic "${topic}" (groupId: "${groupId}")`);
        const consumer = new Kafka.GroupConsumer({
            connectionString: this.kafkaServers,
            groupId,
            logger: {
                logLevel: 4,
                logFunction: this.logFunction,
            },
            idleTimeout: 0,
            handlerConcurrency: concurrency,
        });
        await consumer.init([{
            strategy: new Kafka.DefaultAssignmentStrategy(),
            subscriptions: [topic],
            handler: async (messageSet, handledTopic, partition) => {
                for (const m of messageSet) {
                    // console.log('m>', m);
                    if (this.logger) {
                        this.logger.debug(`Received message from topic ${handledTopic}`);
                    }
                    // console.log('aaa', JSON.parse((m as any).message.value));
                    try {
                        await handler(JSON.parse((m as any).message.value));
                    } catch (err) {
                        this.logger.error(err);
                    }
                    await consumer.commitOffset({
                        topic: handledTopic,
                        partition,
                        offset: (m as any).offset,
                        /*, metadata: 'optional'*/
                    });
                }
            },
        }]);
        this.consumers.push(consumer);
        return async () => {
            await this.closeConsumer(consumer);
        };
    }

    public async closeConsumers() {
        await Promise.all(this.consumers.map((consumer) => this.closeConsumer(consumer)));
    }

    public async destroy() {
        await this.closeConsumers();
        await this.producer.end();
    }

    private async closeConsumer(consumer: Kafka.GroupConsumer) {
        this.consumers = this.consumers.filter((item) => item !== consumer);
        await consumer.end();
    }
}
