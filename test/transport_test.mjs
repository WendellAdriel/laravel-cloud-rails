import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import vm from 'node:vm';

test('heartbeats wait for welcome so AnyCable preserves its replay timestamp', async () => {
    const transports = [];
    class NativeWebSocket extends EventTarget {
        constructor() {
            super();
            transports.push(this);
        }
        send() {}
        close() {}
    }
    const sandbox = {
        WebSocket: NativeWebSocket, EventTarget, Event, MessageEvent, DOMException,
        AbortController, AbortSignal, URL, crypto, setTimeout, clearTimeout, setInterval, clearInterval,
        location: { href: 'https://demo.example/' },
        __laravelCloudWebsockets: { scheme: 'https', host: 'socket.example', port: 443, key: 'public-key' },
        addEventListener() {}, removeEventListener() {},
        fetch: async () => ({ ok: true, json: async () => ({ token: 'session', channels: [] }) }),
    };
    vm.runInNewContext(await readFile(new URL('../lib/laravel_cloud/websockets/transport.js', import.meta.url), 'utf8'), sandbox);
    const socket = new sandbox.WebSocket('/cable', ['actioncable-v1-ext-json']);
    const messages = [];
    socket.addEventListener('message', event => messages.push(JSON.parse(event.data)));
    const receive = async event => {
        transports[0].dispatchEvent(new MessageEvent('message', { data: JSON.stringify(event) }));
        await new Promise(resolve => setImmediate(resolve));
    };
    try {
        await receive({ event: 'pusher:connection_established', data: { socket_id: '1.1' } });
        await receive({ event: 'cloud-heartbeat', data: { time: 200 } });
        assert.deepEqual(messages, []);
        await receive({ event: 'cloud-frame', data: { sequence: 1, frame: { type: 'welcome' } } });
        await receive({ event: 'cloud-heartbeat', data: { time: 203 } });
        assert.deepEqual(messages, [{ type: 'welcome' }, { type: 'ping', message: 203 }]);
    } finally {
        clearInterval(socket.lease);
        clearTimeout(socket.deadline);
    }
});
