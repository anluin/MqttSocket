import {
    ConnAckPacket,
    ConnAckReturnCode,
    ConnectPacket,
    Message as PacketMessage,
    Packet,
    PacketDecodeStream,
    PacketEncodeStream,
    PacketType,
    QualityOfService,
    SubAckReturnCode,
    Subscription
} from "https://deno.land/x/mqttify@0.0.6/protocol/3.1.1/mod.ts";
import { defaults, delay, Payload, Uint16, unwrappedPromise } from "https://deno.land/x/mqttify@0.0.6/utils/mod.ts";


export const PROTOCOL_REGEXP = /^(?<protocol>mqtt)(?<secure>s)?:/;

export class OpenEvent extends Event {

}

export type Message = Omit<PacketMessage, "payload"> & { payload: Payload };

export interface MessageEventInit extends EventInit {
    message: Message;
}

export class MessageEvent extends Event {
    message: Message;

    constructor(type: string, eventInitDict: MessageEventInit) {
        super(type, eventInitDict);
        this.message = eventInitDict.message;
    }
}

export interface PacketEventInit extends EventInit {
    packet: Packet;
}

export class PacketEvent extends Event {
    packet: Packet;

    constructor(type: string, eventInitDict: PacketEventInit) {
        super(type, eventInitDict);
        this.packet = eventInitDict.packet;
    }
}

export class CloseEvent extends Event {

}

export interface SubscribeEventInit extends EventInit {
    subscriptions: Subscription[];
}

export class SubscribeEvent extends Event {
    subscriptions: Subscription[];

    constructor(type: string, eventInitDict: SubscribeEventInit) {
        super(type, eventInitDict);
        this.subscriptions = eventInitDict.subscriptions;
    }
}

export interface UnsubscribeEventInit extends EventInit {
    topics: string[];
}

export class UnsubscribeEvent extends Event {
    topics: string[];

    constructor(type: string, eventInitDict: UnsubscribeEventInit) {
        super(type, eventInitDict);
        this.topics = eventInitDict.topics;
    }
}

export interface MqttSocketEventMap {
    open: OpenEvent;
    message: MessageEvent;
    packet: PacketEvent;
    error: ErrorEvent;
    close: CloseEvent;
    subscribe: SubscribeEvent;
    unsubscribe: UnsubscribeEvent;
}

type HandshakeOptions = Omit<ConnectPacket, "type">;

type ConnectOptions = {
    secure: boolean,
    hostname: string,
    port: number,
} & HandshakeOptions;

export type AuthRequest = Omit<ConnectPacket, "type">;
export type AuthResponse = Omit<ConnAckPacket, "type">;
export type AuthHandler = (request: AuthRequest) => AuthResponse | Promise<AuthResponse>;

export type PublishMessage = Omit<PacketMessage, "payload"> & {
    payload?: Payload[keyof Payload],
};

export enum MqttSocketState {
    Connecting,
    Open,
    Closing,
    Closed,
}

export interface MqttSocket {
    addEventListener<K extends keyof MqttSocketEventMap>(
        type: K,
        listener: (this: WebSocket, ev: MqttSocketEventMap[K]) => unknown,
        options?: boolean | AddEventListenerOptions,
    ): void;

    addEventListener(
        type: string,
        listener: EventListenerOrEventListenerObject,
        options?: boolean | AddEventListenerOptions,
    ): void;

    removeEventListener<K extends keyof MqttSocketEventMap>(
        type: K,
        listener: (this: WebSocket, ev: MqttSocketEventMap[K]) => unknown,
        options?: boolean | EventListenerOptions,
    ): void;

    removeEventListener(
        type: string,
        listener: EventListenerOrEventListenerObject,
        options?: boolean | EventListenerOptions,
    ): void;
}

export class MqttSocket extends EventTarget {
    static readonly OPEN = MqttSocketState.Open;
    static readonly CONNECTING = MqttSocketState.Connecting;
    static readonly CLOSING = MqttSocketState.Closing;
    static readonly CLOSED = MqttSocketState.Closed;

    private state: MqttSocketState;
    private connection?: Deno.Conn;
    private writer?: WritableStreamDefaultWriter<Packet>;
    private packetIdCounter?: number;
    private keepAliveTimeoutId?: number;

    readonly url: URL;
    readonly keepAlive: number;


    constructor(url: string | URL);
    constructor(connection: Deno.Conn, authHandler?: AuthHandler);
    constructor(urlOrConnection: string | URL | Deno.Conn, authHandler?: AuthHandler) {
        super();

        if (urlOrConnection instanceof URL || typeof urlOrConnection === "string") {
            this.url = (
                urlOrConnection instanceof URL
                    ? urlOrConnection
                    : new URL(urlOrConnection)
            );

            const protocolRegExpResult = PROTOCOL_REGEXP.exec(this.url.protocol);

            if (!protocolRegExpResult) {
                throw new Error(`unsupported protocol: "${this.url.protocol.slice(0, -1)}"`);
            }

            this.state = MqttSocketState.Connecting;

            this.connect({
                secure: !!/^mqtt(?<secure>s)?:/.exec(this.url.protocol)?.groups?.secure,
                hostname: this.url.hostname,
                port: (
                    this.url.port
                        ? parseInt(this.url.port)
                        : defaults.port
                ),
                clientId: this.url.searchParams.get("clientId") || `mqttify-${crypto.randomUUID()}`,
                cleanSession: (
                    this.url.searchParams.has("cleanSession")
                        ? this.url.searchParams.get("cleanSession") === "true"
                        : !this.url.searchParams.has("clientId")
                ),
                keepAlive: this.keepAlive = (
                    parseInt(this.url.searchParams.get("keepAlive")!) ||
                    defaults.keepAlive
                ),
                username: this.url.username ? this.url.username : undefined,
                password: this.url.password ? this.url.password : undefined,
            })
                .catch(console.error);
        } else if (authHandler) {
            this.state = MqttSocketState.Connecting;
            this.url = new URL("mqtt://");
            this.keepAlive = -1;
            this.accept(urlOrConnection, authHandler)
                .catch(console.error);
        } else {
            this.state = MqttSocketState.Connecting;
            this.url = new URL("mqtt://");
            this.keepAlive = -1;
            this.link(urlOrConnection)
                .catch(console.error);
        }
    }

    get readyState() {
        return this.state;
    }

    async publish(message: PublishMessage) {
        const id = (
            message.qos ?? 0 > 0
                ? this.nextPacketId()
                : 0 as Uint16
        );


        const payload = Payload.encode(message.payload);

        for (let count = 0; count < defaults.retries.publish; ++count) {
            await this.send(PacketType.Publish, {
                id,
                dup: false,
                message: {
                    ...message,
                    payload,
                },
            });

            switch (message.qos ?? 0) {
                case QualityOfService.atMostOnce:
                    return;
                case QualityOfService.atLeastOnce:
                    try {
                        await this.receive(PacketType.PubAck, defaults.timeouts.puback, id);
                        return;
                    } catch (error) {
                        if (error instanceof Deno.errors.TimedOut) {
                            continue;
                        }

                        throw error;
                    }
                case QualityOfService.exactlyOnce:
                    try {
                        await this.receive(PacketType.PubRec, defaults.timeouts.pubrec, id);

                        for (let count = 0; count < defaults.retries.pubrel; ++count) {
                            await this.send(PacketType.PubRel, { id });

                            try {
                                await this.receive(PacketType.PubComp, defaults.timeouts.pubcomp, id);
                                return;
                            } catch (error) {
                                if (error instanceof Deno.errors.TimedOut) {
                                    continue;
                                }

                                throw error;
                            }
                        }

                        break;
                    } catch (error) {
                        if (error instanceof Deno.errors.TimedOut) {
                            continue;
                        }

                        throw error;
                    }
            }

            break;
        }

        throw new Deno.errors.TimedOut();
    }

    async subscribe(subscriptions: Subscription[]): Promise<SubAckReturnCode[]> {
        const id = this.nextPacketId();

        for (let count = 0; count < defaults.retries.suback; ++count) {
            await this.send(PacketType.Subscribe, {
                id,
                subscriptions,
            });

            try {
                return await (
                    this.receive(PacketType.SubAck, defaults.timeouts.suback, id)
                        .then(({ returnCodes }) => returnCodes)
                );
            } catch (error) {
                if (error instanceof Deno.errors.TimedOut) {
                    continue;
                }

                throw error;
            }
        }

        throw new Deno.errors.TimedOut();
    }

    async unsubscribe(topics: string[]): Promise<void> {
        const id = this.nextPacketId();

        for (let count = 0; count < defaults.retries.unsuback; ++count) {
            await this.send(PacketType.Unsubscribe, {
                id,
                topics,
            });

            try {
                await this.receive(PacketType.UnsubAck, defaults.timeouts.unsuback, id);
                return;
            } catch (error) {
                if (error instanceof Deno.errors.TimedOut) {
                    continue;
                }

                throw error;
            }
        }

        throw new Deno.errors.TimedOut();
    }

    async send(packet: Packet): Promise<void>;
    async send<T extends Packet["type"]>(type: T, data: Omit<Extract<Packet, {
        type: T
    }>, "type">): Promise<void>;
    async send(typeOrPacket: PacketType | Packet, optionalData?: Omit<Packet, "type">) {
        if (typeof typeOrPacket !== "number") {
            await this.writer?.write(typeOrPacket);
        } else {
            await this.writer?.write(<Packet>{
                ...optionalData!,
                type: typeOrPacket,
            });
        }
    }

    async receive(timeout: number): Promise<Packet>;
    async receive<T extends Extract<Packet, {
        id: number
    }>["type"]>(type: T, timeout: number, id?: number): Promise<Extract<Packet, {
        type: T
    }>>;
    async receive<T extends Packet["type"]>(type: T, timeout: number): Promise<Extract<Packet, {
        type: T
    }>>;
    async receive<T extends Packet["type"]>(typeOrTimeout: T | number, optionalTimeout?: number, id?: number) {
        return await new Promise<Extract<Packet, {
            type: T
        }>>((resolve, reject) => {
            const [ type, timeout ] = (
                optionalTimeout !== undefined
                    ? [ typeOrTimeout as Packet["type"], optionalTimeout ]
                    : [ undefined, typeOrTimeout as number ]
            );

            const timeoutId = (
                timeout > 0
                    ? setTimeout(() => {
                        this.removeEventListener("packet", listener);
                        reject(new Deno.errors.TimedOut(JSON.stringify({
                            type,
                            timeout,
                            id,
                        })));
                    }, timeout)
                    : -1
            );

            const listener = (event: PacketEvent) => {
                if ((type === undefined || event.packet.type === type) && (id === undefined || ("id" in event.packet && event.packet.id === id))) {
                    clearTimeout(timeoutId);
                    event.preventDefault();
                    this.removeEventListener("packet", listener);
                    resolve(event.packet as Extract<Packet, {
                        type: T
                    }>);
                }
            };

            this.addEventListener("packet", listener);
        });
    }

    initKeepAliveTimeout() {
        this.keepAliveTimeoutId ?? clearTimeout(this.keepAliveTimeoutId);
        this.keepAliveTimeoutId = setTimeout(async () => {
            await this.send(PacketType.PingReq, {});

            try {
                await this.receive(PacketType.PingResp, defaults.timeouts.pingresp);
                this.initKeepAliveTimeout();
            } catch (error) {
                if (error instanceof Deno.errors.TimedOut) {
                    this.close();
                    return;
                }

                throw error;
            }
        }, Math.max(1000, this.keepAlive) - 100);
    }

    close() {
        switch (this.state) {
            case MqttSocketState.Connecting:
            case MqttSocketState.Open:
                this.state = MqttSocketState.Closing;
                this.dispatchEvent(new CloseEvent("close"));
                this.send(PacketType.Disconnect, {})
                    .then(() => {
                        this.writer = undefined;

                        try {
                            ([ , this.connection ] = [ this.connection, undefined ])[0]
                                ?.close();
                        } catch (_) {
                            // ignore error
                        }
                    });

                break;
        }
    }

    private nextPacketId() {
        return ((this.packetIdCounter = (this.packetIdCounter ?? 0) % 2 ** 16) + 1) as Uint16;
    }

    private async link(connection: Deno.Conn, andThen?: (readable: ReadableStream<Packet>, writable: WritableStream<Packet>) => Promise<void>) {
        const packetDecodeStream = new PacketDecodeStream();
        const packetEncodeStream = new PacketEncodeStream();
        const handleUnload = () => this.close();

        try {
            globalThis.addEventListener("unload", handleUnload);

            const {
                promise,
                reject,
            } = unwrappedPromise();

            await Promise.race([
                connection.readable.pipeTo(packetDecodeStream.writable),
                packetEncodeStream.readable.pipeTo(connection.writable),
                promise,
                (async () => {
                    await andThen?.(packetDecodeStream.readable, packetEncodeStream.writable);

                    this.writer = packetEncodeStream.writable.getWriter();
                    this.state = MqttSocketState.Open;
                    this.connection = connection;

                    this.dispatchEvent(new OpenEvent("open"));

                    for await (const packet of packetDecodeStream.readable) {
                        this.dispatchEvent(new PacketEvent("packet", {
                            packet,
                        }));

                        (async () => {
                            switch (packet.type) {
                                case PacketType.Publish:
                                    qos:
                                        switch (packet.message.qos ?? 0) {
                                            case QualityOfService.atLeastOnce:
                                                await this.send(PacketType.PubAck, packet);

                                                break;
                                            case QualityOfService.exactlyOnce:
                                                for (let count = 0; count < defaults.retries.pubrec; ++count) {
                                                    await this.send(PacketType.PubRec, packet);

                                                    try {
                                                        await this.receive(PacketType.PubRel, defaults.timeouts.pubrel, packet.id);
                                                        await this.send(PacketType.PubComp, packet);

                                                        break qos;
                                                    } catch (error) {
                                                        if (error instanceof Deno.errors.TimedOut) {
                                                            continue;
                                                        }

                                                        throw new error;
                                                    }
                                                }

                                                throw new Deno.errors.TimedOut();
                                        }

                                    this.dispatchEvent(new MessageEvent("message", {
                                        message: {
                                            ...packet.message,
                                            payload: Payload.decode(packet.message.payload),
                                        },
                                    }));

                                    break;
                                case PacketType.Subscribe:
                                    // TODO: expose suback via event?
                                    await this.send(PacketType.SubAck, {
                                        id: packet.id,
                                        returnCodes: packet.subscriptions.map(() =>
                                            SubAckReturnCode.SuccessMaximumQoS_0,
                                        ),
                                    });

                                    this.dispatchEvent(new SubscribeEvent("subscribe", packet));

                                    break;

                                case PacketType.Unsubscribe:
                                    await this.send(PacketType.UnsubAck, packet);

                                    this.dispatchEvent(new UnsubscribeEvent("unsubscribe", packet));

                                    break;

                                case PacketType.PingReq:
                                    await this.send(PacketType.PingResp, {});
                                    break;
                            }
                        })()
                            .catch(reject);
                    }
                })(),
            ]);
        } catch (error) {
            if (this.state !== MqttSocketState.Closing) {
                this.dispatchEvent(new ErrorEvent("error", error));
            }
        } finally {
            if (this.keepAliveTimeoutId) {
                clearTimeout(this.keepAliveTimeoutId);
                this.keepAliveTimeoutId = undefined;
            }

            globalThis.removeEventListener("unload", handleUnload);
            this.close();
            this.state = MqttSocketState.Closed;
        }
    }

    private async accept(connection: Deno.Conn, authHandler: AuthHandler) {
        await this.link(
            connection,
            async (readable, writable) => {
                const writer = writable.getWriter();
                const reader = readable.getReader();

                const packet = await Promise.race([
                    reader.read().then(({ value }) => value),
                    delay(defaults.timeouts.connect),
                ]);

                if (packet?.type !== PacketType.Connect) {
                    throw new Error(`received unexpected packet: #${packet?.type}`);
                }

                const {
                    type: _,
                    ...request
                } = packet;

                const response = await authHandler(request);

                await writer.write({
                    type: PacketType.ConnAck,
                    ...response,
                });

                reader.releaseLock();
                writer.releaseLock();

                if (response.returnCode !== ConnAckReturnCode.ConnectionAccepted) {
                    throw new Error("connection rejected");
                }
            },
        );
    }

    private async connect(options: ConnectOptions) {
        await this.link(
            options.secure
                ? await Deno.connectTls(options)
                : await Deno.connect(options),
            async (readable, writable) => {
                const writer = writable.getWriter();
                const reader = readable.getReader();

                await writer.write({
                    type: PacketType.Connect,
                    ...options,
                });

                this.initKeepAliveTimeout();

                const { value: packet } = await reader.read();

                if (packet?.type !== PacketType.ConnAck) {
                    throw new Error(`received unexpected packet: #${packet?.type}`);
                }

                reader.releaseLock();
                writer.releaseLock();
            },
        );
    }
}
