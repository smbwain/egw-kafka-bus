
import EventEmitter = NodeJS.EventEmitter;

export default function(
    emitter: EventEmitter,
    successEvents: string | string[],
    errorEvents: string | string[],
): Promise<void> {
    successEvents = Array.isArray(successEvents) ? successEvents : [successEvents];
    errorEvents = Array.isArray(errorEvents) ? errorEvents : [errorEvents];

    return new Promise((resolve, reject) => {
        const errorHandler = (err: any) => {
            cleanup();
            reject(err);
        };
        const successHandler = () => {
            cleanup();
            resolve();
        };
        const cleanup = () => {
            for (const errorEvent of errorEvents) {
                emitter.removeListener(errorEvent, errorHandler);
            }
            for (const successEvent of successEvents) {
                emitter.removeListener(successEvent, successHandler);
            }
        };
        for (const successEvent of successEvents) {
            emitter.on(successEvent, successHandler);
        }
        for (const errorEvent of errorEvents) {
            emitter.on(errorEvent, errorHandler);
        }
    });
}
