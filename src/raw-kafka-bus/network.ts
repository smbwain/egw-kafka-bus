import {Server, createServer, createConnection, AddressInfo} from 'net';

import eventsToPromise from './events-to-promise';

export function createNetworkServer(
    {host, port}: {
        host: string;
        port: number;
    },
    handler: (data: any) => void,
) {
    let server: Server;

    const listen = async () => {
        server = createServer(socket => {
            const data: string[] = [];
            socket.setEncoding('utf8');
            socket.on('data', (chunk: string) => data.push(chunk));
            socket.on('end', () => {
                handler(JSON.parse(data.join('')));
            });
        });
        await new Promise((resolve, reject) => {
            server.listen({host, port}, ((err: any) => {
                err ? reject(err) : resolve();
            }) as any);
        });
        port = (server.address() as AddressInfo).port;
    };

    const close = async () => {
        if (server) {
            await new Promise((resolve, reject) => {
                server.close(err => err ? reject(err) : resolve());
            });
            server = null;
        }
    };

    const getAddress = () => {
        return {host, port};
    };

    return {
        listen,
        close,
        getAddress,
    };
}

export async function networkRequest(
    {host, port}: {
        host: string;
        port: number;
    },
    data: any,
) {
    const connection = createConnection({
        host,
        port
    });
    await eventsToPromise(connection, 'connect', ['error', 'timeout']);
    connection.end(JSON.stringify(data));
    await eventsToPromise(connection, 'close', ['error', 'timeout']);
}
