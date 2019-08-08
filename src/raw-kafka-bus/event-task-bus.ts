import {Logger, Profiler} from 'msv-logger';
import * as serializeError from 'serialize-error';

import {MessageBus, Unsubscriber} from './message-bus';
import {createNetworkServer, networkRequest} from './network';

export {Unsubscriber} from './message-bus';

interface Defer<T> {
    resolve: (value: T) => void;
    reject: (err: Error) => void;
    timeout: number;
    profiler: Profiler;
}

interface TaskSendData<D> {
    key: string;
    data: D;
    // successEvent?: string,
    // errorEvent?: string,
    rpcSender?: {
        host: string;
        port: number;
    };
}

const parseAddress = (addr: string) => {
    const arr = addr.split(':');
    return {
        host: arr[0],
        port: parseInt(arr[1], 10) || undefined,
    };
};

const deserializeError = (error: any): Error => {
    const {message, ...rest} = error;
    const err = new Error(message);
    for (const propName in rest) {
        if (rest.hasOwnProperty(propName)) {
            (err as any)[propName] = rest[propName];
        }
    }
    return err;
};

export const createEventTaskBus = async ({
    kafkaServers,
    groupId,
    myAddress: myAddressStr,
    myExternalAddress: myExternalAddressStr,
    logger,
}: {
    kafkaServers: string;
    groupId: string;
    myAddress: string;
    myExternalAddress?: string;
    logger: Logger;
}) => {
    const myAddress = parseAddress(myAddressStr);

    const waitingTasks: {
        [key: string]: Defer<any>;
    } = {};

    const netServer = createNetworkServer(myAddress, ({key, error, data}: {
        key: string;
        error?: any;
        data: any;
    }) => {
        const def = waitingTasks[key];
        if (!def) {
            logger.error(`No task found. Key: ${key}`);
            return;
        }
        if (def.timeout) {
            clearTimeout(def.timeout);
        }
        delete waitingTasks[key];
        def.profiler(`response received`);
        if (error) {
            def.reject(deserializeError(error));
        } else {
            def.resolve(data);
        }
    });
    const messageBus = new MessageBus({
        kafkaServers,
        logger: logger.sub({tag: 'message-bus'}),
    });

    await netServer.listen();
    const myRealAddress = netServer.getAddress();
    const myExternalAddress = {
        ...myRealAddress,
        ...myExternalAddressStr ? parseAddress(myExternalAddressStr) : {},
    };
    logger.debug(
        `RPC listener is on ${myRealAddress.host}:${myRealAddress.port} ` +
        `(externally: ${myExternalAddress.host}:${myExternalAddress.port})`,
    );
    // const myExternalAddress =  ?  : myAddress;
    await messageBus.init();

    async function destroy(): Promise<void> {
        await messageBus.closeConsumers();
        // todo: await for current jobs finish
        await netServer.close();
        await messageBus.destroy();
    }

    async function registerEventListener<D>(
        name: string,
        {concurrency}: {concurrency: number},
        handler: (data: D) => Promise<void>,
    ): Promise<Unsubscriber> {
        return messageBus.consume(
            `event_${name}`,
            {
                groupId,
                concurrency,
            },
            ({data}) => handler(data),
        );
    }

    let taskCnt = 1;
    async function registerTaskWorker<D, R>(
        name: string,
        {concurrency}: {concurrency: number},
        handler: (data: D) => Promise<R>,
    ): Promise<Unsubscriber> {
        return messageBus.consume(
            `task_${name}`,
            {
                groupId,
                concurrency,
            },
            async ({key, rpcSender, data/*, successEvent, errorEvent*/}) => {
                let res;
                const profiler = logger.profiler(`in-task-${taskCnt++}`);
                profiler(`start processing ${name}`);
                try {
                    res = await handler(data);
                } catch (err) {
                    const error = serializeError(err);
                    if (rpcSender) {
                        profiler('sending error response');
                        await networkRequest(rpcSender, {
                            key,
                            error,
                        });
                        profiler(`finish error response`);
                    }
                    /* if (errorEvent) {
                        // profiler('sending error event');
                        await send(errorEvent, {
                            key,
                            request: data,
                            error
                        });
                        // profiler('finish sending error event');
                    }*/
                    return;
                }
                if (rpcSender) {
                    profiler('sending response');
                    await networkRequest(rpcSender, {
                        key,
                        data: res,
                    });
                    profiler('finish response');
                }
                /*if (successEvent) {
                    profiler('sending event');
                    await send(successEvent, {
                        key,
                        request: data,
                        response: res
                    });
                    profiler('finish sending event');
                }*/
            },
        );
    }

    async function sendEvent<D>(name: string, data: D): Promise<void> {
        await messageBus.send(`event_${name}`, {data});
    }

    function runTask<D, R>(name: string, data: D, options: {
        wait: number,
    }): Promise<R>;
    function runTask<D>(name: string, data: D, options: {
        wait: false,
    }): Promise<void>;
    async function runTask<D, R>(name: string, data: D, {wait}: {
        wait: number | false,
    }): Promise<R> {
        let promise: Promise<R>;
        const key = `${name}:${Math.random().toString().slice(2)}`;
        const sendData: TaskSendData<D> = {
            key,
            data,
        };
        if (wait !== false) {
            sendData.rpcSender = myExternalAddress;
            promise = new Promise((resolve, reject) => {
                const profiler = logger.profiler(`ext-task-${key}`);
                waitingTasks[key] = {
                    resolve,
                    reject,
                    profiler,
                    timeout: setTimeout(() => {
                        delete waitingTasks[key];
                        profiler(`delete`);
                        reject(new Error(`Task response timeout exceeded. Task: ${name}`));
                    }, wait) as any,
                };
                profiler(`create new task ${name}`);
            });
        }
        await messageBus.send(`task_${name}`, sendData);
        return await promise;
    }

    return {
        registerEventListener,
        registerTaskWorker,
        sendEvent,
        runTask,
        destroy,
    };
};
