(() => {
    if (globalThis.WebSocket.__laravelCloudBridge) return;
    const config = globalThis.__laravelCloudWebsockets;
    const NativeWebSocket = globalThis.WebSocket;
    const endpoint = '/__laravel_cloud/cable';

    class CableSocket extends EventTarget {
        constructor(url, protocols) {
            super();
            this.url = new URL(url, location.href).href;
            this.protocol = 'actioncable-v1-json';
            this.extensions = '';
            this.readyState = 0;
            this.bufferedAmount = 0;
            this.binaryType = 'blob';
            this.sequence = 0;
            this.joined = new Map();
            this.queue = Promise.resolve();
            this.abort = new AbortController();
            this.onPageHide = () => this.close();
            globalThis.addEventListener("pagehide", this.onPageHide, { once: true });
            this.transport = new NativeWebSocket(
                `${config.scheme === 'https' ? 'wss' : 'ws'}://${config.host}:${config.port}/app/${encodeURIComponent(config.key)}?protocol=7&client=rails-on-cloud&version=1`
            );
            this.transport.addEventListener('message', event => {
                try {
                    this.receive(JSON.parse(event.data)).catch(error => this.fail(error));
                } catch (error) {
                    this.fail(error);
                }
            });
            this.transport.addEventListener('close', event => this.finish(event.code, event.reason, event.wasClean));
            this.transport.addEventListener('error', () => this.fail(new Error('WebSocket backend unavailable')));
            this.deadline = setTimeout(() => this.fail(new Error('Connection timed out')), 12000);
        }

        async request(command, keepalive = false) {
            const response = await fetch(endpoint, {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ...command, token: this.token, request_id: crypto.randomUUID() }),
                cache: 'no-store',
                keepalive,
                signal: keepalive ? AbortSignal.timeout(5000) : AbortSignal.any([this.abort.signal, AbortSignal.timeout(10000)]),
            });
            const result = await response.json();
            if (!response.ok) {
                const error = new Error(result.error || `Channel request failed: HTTP ${response.status}`);
                error.terminal = [400, 401, 403, 404, 413, 415].includes(response.status);
                throw error;
            }
            return result;
        }

        async receive(event) {
            const data = typeof event.data === 'string' ? JSON.parse(event.data) : event.data;
            if (event.event === 'pusher:connection_established') {
                const url = new URL(this.url);
                const result = await this.request({ command: 'connect', socket_id: data.socket_id, url: url.pathname + url.search });
                this.token = result.token;
                if (this.readyState > 1) {
                    this.request({ command: 'disconnect' }, true).catch(() => {});
                    return;
                }
                const pending = result.channels.map(channel => new Promise(resolve => {
                    this.joined.set(channel.name, resolve);
                    this.transport.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel: channel.name, auth: channel.auth } }));
                }));
                await Promise.all(pending);
                if (this.readyState > 1) return;
                clearTimeout(this.deadline);
                this.readyState = 1;
                this.emit(new Event('open'));
                await this.request({ command: 'ready' });
                this.lease = setInterval(() => this.send(JSON.stringify({ command: 'lease' })), 30000);
                return;
            }
            if (event.event === 'pusher_internal:subscription_succeeded') {
                this.joined.get(event.channel)?.();
                return;
            }
            if (event.event === 'pusher:ping') {
                this.transport.send(JSON.stringify({ event: 'pusher:pong', data: {} }));
                return;
            }
            if (event.event === 'pusher:error' || event.event === 'pusher:subscription_error') {
                throw new Error('WebSocket subscription failed');
            }
            if (event.event === 'cloud-heartbeat' && this.readyState === 1) {
                this.message({ type: 'ping', message: data.time });
            }
            if (event.event === 'cloud-frame' && this.readyState === 1) {
                if (data.sequence <= this.sequence) return;
                if (data.sequence !== this.sequence + 1) throw new Error('Connection stream interrupted');
                this.sequence = data.sequence;
                this.message(data.frame);
            }
        }

        send(raw) {
            if (this.readyState !== 1) throw new DOMException('WebSocket is not open', 'InvalidStateError');
            const command = JSON.parse(raw);
            this.queue = this.queue.then(() => this.request(command)).catch(error => this.fail(error));
        }

        message(frame) {
            this.emit(new MessageEvent('message', { data: JSON.stringify(frame) }));
        }

        emit(event) {
            this[`on${event.type}`]?.(event);
            this.dispatchEvent(event);
        }

        fail(error) {
            if (this.readyState > 1) return;
            this.emit(new Event('error'));
            if (error.terminal && this.readyState === 1) this.message({ type: 'disconnect', reason: error.message, reconnect: false });
            this.finish(error.terminal ? 1008 : 1006, error.message, false);
            this.transport.close();
        }

        close(code = 1000, reason = '') {
            if (this.readyState > 1) return;
            this.readyState = 2;
            if (this.token) this.request({ command: 'disconnect' }, true).catch(() => {});
            this.transport.close();
            this.finish(code, reason, true);
        }

        finish(code = 1006, reason = '', wasClean = false) {
            if (this.readyState === 3) return;
            this.readyState = 3;
            clearTimeout(this.deadline);
            clearInterval(this.lease);
            globalThis.removeEventListener("pagehide", this.onPageHide);
            this.abort.abort();
            this.emit(new CloseEvent('close', { code, reason, wasClean }));
        }
    }

    function CloudWebSocket(url, protocols = []) {
        if (!new.target) throw new TypeError('WebSocket constructor requires new');
        const names = Array.isArray(protocols) ? protocols : [protocols];
        if (names.includes('actioncable-v1-json')) return new CableSocket(url, names);
        return new NativeWebSocket(url, protocols);
    }

    for (const [name, value] of Object.entries({ CONNECTING: 0, OPEN: 1, CLOSING: 2, CLOSED: 3 })) {
        Object.defineProperty(CloudWebSocket, name, { value, enumerable: true });
        Object.defineProperty(CableSocket.prototype, name, { value });
    }
    Object.defineProperty(CloudWebSocket, Symbol.hasInstance, { value: value => value instanceof NativeWebSocket || value instanceof CableSocket });
    CloudWebSocket.__laravelCloudBridge = true;
    CloudWebSocket.prototype = NativeWebSocket.prototype;
    globalThis.WebSocket = CloudWebSocket;
})();
