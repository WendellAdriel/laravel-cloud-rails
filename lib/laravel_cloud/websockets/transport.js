(() => {
    const config = globalThis.__laravelCloudWebsockets;
    const NativeWebSocket = globalThis.WebSocket;
    const endpoint = '/__laravel_cloud/cable';

    class CableSocket extends EventTarget {
        constructor(url, protocols) {
            super();
            this.url = url;
            this.protocol = protocols.includes('actioncable-v1-json') ? 'actioncable-v1-json' : protocols[0];
            this.readyState = 0;
            this.bufferedAmount = 0;
            this.extensions = '';
            this.channels = new Map();
            this.joined = new Map();
            this.queue = Promise.resolve();
            this.transport = new NativeWebSocket(
                `${config.scheme === 'https' ? 'wss' : 'ws'}://${config.host}:${config.port}/app/${encodeURIComponent(config.key)}?protocol=7&client=rails-on-cloud&version=prototype`
            );
            this.transport.addEventListener('message', event => {
                this.receive(JSON.parse(event.data)).catch(error => this.fail(error));
            });
            this.transport.addEventListener('close', () => this.finish());
            this.transport.addEventListener('error', () => this.fail(new Error('Pusher connection failed')));
        }

        async request(command, keepalive = false) {
            const response = await fetch(endpoint, {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ...command, token: this.token }),
                keepalive,
            });
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.error || `Channel request failed: HTTP ${response.status}`);
            }
            return result;
        }

        async receive(event) {
            const data = typeof event.data === 'string' ? JSON.parse(event.data) : event.data;
            if (event.event === 'pusher:connection_established') {
                const url = new URL(this.url, globalThis.location.href);
                const result = await this.request({ command: 'connect', socket_id: data.socket_id, url: url.pathname + url.search });
                if (this.readyState > 1) return;
                this.token = result.token;
                this.readyState = 1;
                this.emit(new Event('open'));
                result.frames.forEach(frame => this.message(frame));
                this.heartbeat = setInterval(() => this.send(JSON.stringify({ command: 'ping' })), 3000);
                return;
            }
            if (event.event === 'pusher:ping') {
                this.transport.send(JSON.stringify({ event: 'pusher:pong', data: {} }));
                return;
            }
            if (event.event === 'pusher_internal:subscription_succeeded') {
                this.joined.get(event.channel)?.resolve();
                return;
            }
            if (event.event === 'pusher:error' || event.event === 'pusher:subscription_error') {
                throw new Error(data.message || JSON.stringify(data));
            }
            if (event.event === 'cloud-message') {
                for (const identifier of this.channels.get(event.channel) || []) {
                    this.message({ identifier, message: data });
                }
            }
        }

        send(raw) {
            if (this.readyState !== 1) throw new Error('WebSocket is not open');
            const command = JSON.parse(raw);
            this.queue = this.queue.then(async () => {
                const result = await this.request(command);
                await this.reconcile(result.subscriptions);
                result.frames.forEach(frame => this.message(frame));
            }).catch(error => this.fail(error));
        }

        async reconcile(subscriptions) {
            const desired = new Map();
            for (const [identifier, streams] of Object.entries(subscriptions)) {
                for (const stream of streams) {
                    if (!desired.has(stream.name)) desired.set(stream.name, { ...stream, identifiers: new Set() });
                    desired.get(stream.name).identifiers.add(identifier);
                }
            }
            for (const name of this.channels.keys()) {
                if (desired.has(name)) continue;
                this.transport.send(JSON.stringify({ event: 'pusher:unsubscribe', data: { channel: name } }));
                this.joined.delete(name);
            }
            this.channels = new Map([...desired].map(([name, stream]) => [name, stream.identifiers]));
            const pending = [];
            for (const [name, stream] of desired) {
                if (!this.joined.has(name)) {
                    let resolve;
                    const promise = new Promise(done => { resolve = done; });
                    this.joined.set(name, { promise, resolve });
                    this.transport.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel: name, auth: stream.auth } }));
                }
                pending.push(this.joined.get(name).promise);
            }
            await Promise.all(pending);
        }

        message(frame) {
            this.emit(new MessageEvent('message', { data: JSON.stringify(frame) }));
        }

        emit(event) {
            this[`on${event.type}`]?.(event);
            this.dispatchEvent(event);
        }

        fail(error) {
            console.error('Laravel Cloud WebSockets:', error);
            this.emit(new Event('error'));
            if (this.readyState === 1) this.message({ type: 'disconnect', reason: error.message, reconnect: false });
            this.close();
        }

        close() {
            if (this.readyState > 1) return;
            this.readyState = 2;
            clearInterval(this.heartbeat);
            if (this.token) this.request({ command: 'disconnect' }, true).catch(error => console.error(error));
            this.transport.close();
            this.finish();
        }

        finish() {
            if (this.readyState === 3) return;
            this.readyState = 3;
            clearInterval(this.heartbeat);
            this.emit(new Event('close'));
        }
    }

    function CloudWebSocket(url, protocols = []) {
        const names = Array.isArray(protocols) ? protocols : [protocols];
        if (names.some(name => name.startsWith('actioncable-'))) return new CableSocket(url, names);
        return new NativeWebSocket(url, protocols);
    }

    Object.assign(CloudWebSocket, { CONNECTING: 0, OPEN: 1, CLOSING: 2, CLOSED: 3 });
    CloudWebSocket.prototype = NativeWebSocket.prototype;
    globalThis.WebSocket = CloudWebSocket;
})();
