import {asyncInitializer, Context, _logger} from 'egw';

import {createEventTaskBus, Unsubscriber} from './raw-kafka-bus/event-task-bus';

export interface Bus {
    sendEvent<D>(name: string, data: D): Promise<void>;
    runTask<D, R>(name: string, data: D, options: {
        wait: number,
    }): Promise<R>;
    runTask<D>(name: string, data: D, options: {
        wait: false,
    }): Promise<void>;
    registerEventListener<D>(
        name: string,
        options: {concurrency: number},
        handler: (ctx: Context, data: D) => Promise<void>,
    ): Promise<Unsubscriber>;
    registerTaskWorker<D, R>(
        name: string,
        options: {concurrency: number},
        handler: (ctx: Context, data: D) => Promise<R>,
    ): Promise<Unsubscriber>;
}

export const [_bus, _initBus] = asyncInitializer(async (
    baseContext,
    {groupId, kafkaServers, myAddress, myExternalAddress}: {
        groupId: string;
        kafkaServers: string;
        myAddress: string;
        myExternalAddress?: string;
    },
): Promise<Bus> => {
    const {sendEvent, runTask, registerEventListener, registerTaskWorker, destroy} = await createEventTaskBus({
        groupId,
        kafkaServers,
        myAddress,
        myExternalAddress,
        logger: _logger(baseContext).sub({tag: 'KafkaBridge'}),
    });
    baseContext.addDestroyHook(async () => {
        await destroy();
    });
    return {
        sendEvent,
        runTask,
        registerEventListener: <D>(
            name: string,
            options: { concurrency: number },
            handler: (ctx: Context, data: D) => Promise<void>,
        ): Promise<Unsubscriber> => {
            return registerEventListener<D>(name, options, async (data) => {
                const ctx = baseContext.sub();
                try {
                    await handler(ctx, data);
                } finally {
                    await ctx.destroy();
                }
            });
        },
        registerTaskWorker: <D, R>(
            name: string,
            options: { concurrency: number },
            handler: (ctx: Context, data: D) => Promise<R>,
        ): Promise<Unsubscriber> => {
            return registerTaskWorker<D, R>(name, options, async (data) => {
                const ctx = baseContext.sub();
                try {
                    return await handler(ctx, data);
                } finally {
                    await ctx.destroy();
                }
            });
        },
    };
});
