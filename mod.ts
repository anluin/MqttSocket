import { ConnAckPacket, ConnAckReturnCode, ConnectPacket, Message, Packet, PacketDecoderStream, PacketEncoderStream, PacketType, QualityOfService, SubAckReturnCode, } from "https://deno.land/x/mqttify@0.0.9/mod.ts";

import { defaults } from "https://deno.land/x/mqttify@0.0.9/defaults.ts";


export { QualityOfService, SubAckReturnCode, ConnAckReturnCode } from "https://deno.land/x/mqttify@0.0.9/mod.ts";


export interface ItoJSON {
    toJSON(): JSONValue;
}

export type JSONValue =
    | string
    | number
    | boolean
    | {
    [_: string]: JSONValue
}
    | Array<JSONValue>
    | ItoJSON;

export interface OpenEventInit extends EventInit {
    clientId: string,
    cleanSession: boolean,
    sessionPresent: boolean,
    keepAlive: number,
    will?: PublishedMessage;
}

export class OpenEvent extends Event {
    clientId: string;
    cleanSession: boolean;
    sessionPresent: boolean;
    keepAlive: number;
    will?: PublishedMessage;

    constructor(type: string, eventInitDict: OpenEventInit) {
        super(type, eventInitDict);
        this.clientId = eventInitDict.clientId;
        this.cleanSession = eventInitDict.cleanSession;
        this.sessionPresent = eventInitDict.sessionPresent;
        this.keepAlive = eventInitDict.keepAlive;
        this.will = eventInitDict.will;
    }
}

export type PublishedMessage = Omit<Message, "payload"> & {
    payload: {
        bytes: Uint8Array,
        text: string,
        json: JSONValue,
    },
};

export interface MessageEventInit extends EventInit {
    message: PublishedMessage;
}

export class MessageEvent extends Event {
    message: PublishedMessage;

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

export type Subscription = {
    topic: string,
    qos?: QualityOfService,
};

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

export type AuthRequest = Omit<ConnectPacket, "type">;
export type AuthResponse = Omit<ConnAckPacket, "type">;
export type AuthHandler = (request: AuthRequest) => AuthResponse | Promise<AuthResponse>;

export type PublishMessage = {
    topic: string,
    payload?: string | Uint8Array | JSONValue | {
        bytes?: Uint8Array,
    } | {
        text?: string,
    } | {
        json?: JSONValue,
    },
    qos?: QualityOfService,
    retain?: boolean,
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

export type MqttSocketConnectOptions = {
    clientId?: string,
    cleanSession?: boolean,
    keepAlive?: number,
    will?: PublishMessage,
};

export type MqttSocketUrlOptions = {
    secure: boolean,
    hostname: string,
    port?: number,
    username?: string,
    password?: string,
};

type HandshakeOptions = Omit<ConnectPacket, "type">;

type ConnectOptions = {
    secure: boolean,
    hostname: string,
    port: number,
} & HandshakeOptions;

export const textEncoder = new TextEncoder();
export const textDecoder = new TextDecoder();
export const PROTOCOL_REGEXP = /^(?<protocol>mqtt)(?<secure>s)?:/;

export class MqttSocket extends EventTarget {
    static readonly OPEN = MqttSocketState.Open;
    static readonly CONNECTING = MqttSocketState.Connecting;
    static readonly CLOSING = MqttSocketState.Closing;
    static readonly CLOSED = MqttSocketState.Closed;
    readonly url: URL;
    readonly keepAlive: number;
    readonly closed: Promise<void>;
    private readonly timeoutIds = new Set<number>();
    private state: MqttSocketState;
    private connection?: Deno.Conn;
    private writer?: WritableStreamDefaultWriter<Packet>;
    private packetIdCounter?: number;
    private keepAliveTimeoutId?: number;

    constructor(url: string | URL, connectOptions?: MqttSocketConnectOptions);
    constructor(connection: Deno.Conn, authHandler?: AuthHandler);
    constructor(urlOrConnection: string | URL | Deno.Conn, authHandlerOrConnectOptions?: AuthHandler | MqttSocketConnectOptions) {
        super();

        const [ authHandler, connectOptions ] = (
            authHandlerOrConnectOptions instanceof Function
                ? [ authHandlerOrConnectOptions, undefined ]
                : [ undefined, authHandlerOrConnectOptions ]
        );

        if (urlOrConnection instanceof URL || typeof urlOrConnection === "string") {
            const urlOptions = decodeUrlOptions(this.url = (
                urlOrConnection instanceof URL
                    ? urlOrConnection
                    : new URL(urlOrConnection)
            ));

            this.state = MqttSocketState.Connecting;
            this.keepAlive = connectOptions?.keepAlive ?? defaults.keepAlive;
            this.closed = this.connect({
                protocol: {
                    name: "MQTT",
                    level: 4,
                },
                clientId: connectOptions?.clientId || `mqttify-${crypto.randomUUID()}`,
                cleanSession: connectOptions?.cleanSession ?? !connectOptions?.clientId,
                keepAlive: (
                    connectOptions?.keepAlive ??
                    defaults.keepAlive
                ),
                port: defaults.port,
                will: connectOptions?.will && encodePublishMessage(connectOptions.will),
                ...urlOptions,
            })
                .catch(console.error);
        } else if (authHandler) {
            this.state = MqttSocketState.Connecting;
            this.url = new URL("mqtt://");
            this.keepAlive = -1;
            this.closed = this.accept(urlOrConnection, authHandler)
                .catch(console.error);
        } else {
            this.state = MqttSocketState.Connecting;
            this.url = new URL("mqtt://");
            this.keepAlive = -1;
            this.closed = this.link(urlOrConnection)
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
                : 0
        );

        for (let count = 0; count < defaults.retries.publish; ++count) {
            await this.send(PacketType.Publish, {
                id,
                dup: !!count,
                ...encodePublishMessage(message),
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

    async subscribe(topic: string, qos?: QualityOfService): Promise<SubAckReturnCode> ;
    async subscribe(subscriptions: Subscription[]): Promise<SubAckReturnCode[]> ;
    async subscribe(topicOrSubscriptions: string | Subscription[], optionalQos?: QualityOfService): Promise<SubAckReturnCode | SubAckReturnCode[]> {
        if (topicOrSubscriptions instanceof Array) {
            const id = this.nextPacketId();

            for (let count = 0; count < defaults.retries.suback; ++count) {
                await this.send(PacketType.Subscribe, {
                    id,
                    subscriptions: topicOrSubscriptions.map((subscription) => ({
                        topic: subscription.topic,
                        qos: (
                            subscription.qos ??
                            QualityOfService.atMostOnce
                        ),
                    })),
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
        } else {
            return (await this.subscribe([ {
                topic: topicOrSubscriptions,
                qos: optionalQos,
            } ]))[0];
        }
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
        let timeoutId: number | undefined;

        return await new Promise<Extract<Packet, {
            type: T
        }>>((resolve, reject) => {
            const [ type, timeout ] = (
                optionalTimeout !== undefined
                    ? [ typeOrTimeout as Packet["type"], optionalTimeout ]
                    : [ undefined, typeOrTimeout as number ]
            );

            if (timeout > 0) {
                this.timeoutIds.add(timeoutId = setTimeout(() => {
                    this.removeEventListener("packet", listener);
                    this.timeoutIds.delete(timeoutId!);
                    timeoutId = undefined;

                    reject(new Deno.errors.TimedOut(JSON.stringify({
                        type,
                        timeout,
                        id,
                    })));
                }, timeout));
            }

            const listener = (event: PacketEvent) => {
                if ((type === undefined || event.packet.type === type) && (id === undefined || ("id" in event.packet && event.packet.id === id))) {
                    this.removeEventListener("packet", listener);
                    event.preventDefault();

                    if (timeoutId) {
                        clearTimeout(timeoutId);
                        this.timeoutIds.delete(timeoutId);
                    }

                    resolve(event.packet as Extract<Packet, {
                        type: T
                    }>);
                }
            };

            this.addEventListener("packet", listener);
        });
    }

    initKeepAliveTimeout() {
        if (this.keepAliveTimeoutId) {
            this.timeoutIds.delete(this.keepAliveTimeoutId);
            clearTimeout(this.keepAliveTimeoutId);
        }

        this.keepAliveTimeoutId = setTimeout(async () => {
            this.timeoutIds.delete(this.keepAliveTimeoutId!);
            this.keepAliveTimeoutId = undefined;

            await this.send(PacketType.PingReq, {});

            try {
                await this.receive(PacketType.PingResp, defaults.timeouts.pingresp);
                this.initKeepAliveTimeout();
            } catch (error) {
                if (error instanceof Deno.errors.TimedOut) {
                    await this.close();
                    return;
                }

                throw error;
            }
        }, Math.max(1000, this.keepAlive) - 100);
    }

    async disconnect() {
        await this.send(PacketType.Disconnect, {});
        await this.close();
    }

    close() {
        switch (this.state) {
            case MqttSocketState.Connecting:
            case MqttSocketState.Open:
                this.state = MqttSocketState.Closing;
                this.writer = undefined;

                (
                    [ , this.connection ] =
                        [ this.connection, undefined ]
                )[0]?.close();

                break;
        }

        for (const timeoutId of this.timeoutIds) {
            clearTimeout(timeoutId);
        }

        this.timeoutIds.clear();
    }

    private nextPacketId() {
        return ((this.packetIdCounter = (this.packetIdCounter ?? 0) % 2 ** 16) + 1);
    }

    private async link(connection: Deno.Conn, andThen?: (readable: ReadableStream<Packet>, writable: WritableStream<Packet>) => Promise<void>) {
        const packetDecoderStream = new PacketDecoderStream();
        const packetEncoderStream = new PacketEncoderStream();
        const handleUnload = () => this.close();

        try {
            globalThis.addEventListener("unload", handleUnload);

            await Promise.race([
                connection.readable.pipeTo(packetDecoderStream.writable),
                packetEncoderStream.readable.pipeTo(connection.writable),
                (async () => {
                    await andThen?.(packetDecoderStream.readable, packetEncoderStream.writable);

                    this.writer = packetEncoderStream.writable.getWriter();
                    this.state = MqttSocketState.Open;
                    this.connection = connection;

                    for await (const packet of packetDecoderStream.readable) {
                        this.dispatchEvent(new PacketEvent("packet", {
                            packet,
                        }));

                        (async () => {
                            switch (packet.type) {
                                case PacketType.Publish: {
                                    qos:
                                        switch (packet.qos ?? 0) {
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
                                        message: decodePublishedMessage(packet),
                                    }));

                                    break;
                                }
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
                            .catch(console.error);
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
            this.dispatchEvent(new CloseEvent("close"));
        }
    }

    private async accept(connection: Deno.Conn, authHandler: AuthHandler) {
        await this.link(
            connection,
            async (readable, writable) => {
                const writer = writable.getWriter();
                const reader = readable.getReader();

                let timeoutId = -1;

                const connectPacket = await Promise.race([
                    reader.read().then(({ value }) => value),
                    new Promise<undefined>(resolve => timeoutId = setTimeout(resolve, defaults.timeouts.connect)),
                ])
                    .finally(() => clearTimeout(timeoutId));

                if (connectPacket?.type !== PacketType.Connect) {
                    throw new Error(`received unexpected packet: #${connectPacket?.type}`);
                }

                const {
                    type: _,
                    ...authRequest
                } = connectPacket;

                const authResponse = await authHandler(authRequest);

                await writer.write({
                    type: PacketType.ConnAck,
                    ...authResponse,
                });

                reader.releaseLock();
                writer.releaseLock();

                if (authResponse.returnCode !== ConnAckReturnCode.ConnectionAccepted) {
                    throw new Error("connection rejected");
                }

                this.dispatchEvent(new OpenEvent("open", {
                    clientId: connectPacket.clientId,
                    cleanSession: connectPacket.cleanSession,
                    sessionPresent: authResponse.sessionPresent,
                    keepAlive: connectPacket.keepAlive,
                    will: connectPacket.will && decodePublishedMessage(connectPacket.will),
                }));
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

                const { value: connackPacket } = await reader.read();

                if (connackPacket?.type !== PacketType.ConnAck) {
                    throw new Error(`received unexpected packet: #${connackPacket?.type}`);
                }

                reader.releaseLock();
                writer.releaseLock();

                if (connackPacket.returnCode !== ConnAckReturnCode.ConnectionAccepted) {
                    throw new Deno.errors.ConnectionRefused(undefined, { cause: connackPacket.returnCode });
                }

                this.dispatchEvent(new OpenEvent("open", {
                    clientId: options.clientId,
                    cleanSession: options.cleanSession,
                    sessionPresent: connackPacket.sessionPresent,
                    keepAlive: options.keepAlive,
                    will: options.will && decodePublishedMessage(options.will),
                }));
            },
        );
    }
}

export const encodePublishMessage = (message: PublishMessage): Message => {
    let payload = message.payload;

    if (typeof payload === "object") {
        if ("bytes" in payload) {
            payload = payload.bytes;
        } else if ("text" in payload) {
            payload = payload.text;
        } else if ("json" in payload) {
            payload = payload.json;
        }
    }

    if (!(payload instanceof Uint8Array)) {
        if (typeof payload !== "string") {
            payload = JSON.stringify(payload);
        }

        if (typeof payload === "string") {
            payload = textEncoder.encode(payload);
        }
    }

    return {
        topic: message.topic,
        payload: payload,
        qos: message.qos ?? QualityOfService.atMostOnce,
        retain: false,
    };
};

export const decodePublishedMessage = (message: Message): PublishedMessage => {
    const {
        topic,
        payload,
        qos,
        retain,
    } = message;

    return {
        qos,
        retain,
        topic,
        payload: {
            bytes: payload,
            get text() {
                return textDecoder.decode(this.bytes);
            },
            set text(value: string) {
                this.bytes = textEncoder.encode(value);
            },
            get json() {
                return JSON.parse(this.text);
            },
            set json(value: JSONValue) {
                this.text = JSON.stringify(value);
            },
        },
    };
};

export const encodeUrlOptions = (options: MqttSocketUrlOptions) => {
    const url = new URL(`${options.secure ? "mqtts" : "mqtt"}://${options.hostname}:${options.port ?? defaults.port}`);

    if (options.username !== undefined) url.username = options.username;
    if (options.password !== undefined) url.password = options.password;

    return url;
};

export const decodeUrlOptions = (url: string | URL): MqttSocketUrlOptions => {
    url = url instanceof URL ? url : new URL(url);

    const protocolRegExpResult = PROTOCOL_REGEXP.exec(url.protocol);

    if (!protocolRegExpResult) {
        throw new Error(`unsupported protocol: "${url.protocol.slice(0, -1)}"`);
    }

    return {
        secure: !!protocolRegExpResult?.groups?.secure,
        hostname: url.hostname,
        port: (
            url.port
                ? parseInt(url.port)
                : defaults.port
        ),
        username: url.username ? url.username : undefined,
        password: url.password ? url.password : undefined,
    }
};
