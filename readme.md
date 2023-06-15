# MqttSocket

> A high-level `WebSocket`-Like abstraction of the [mqttify](https://deno.land/x/mqttify@0.0.5) implementation of the [MQTT v3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.pdf) protocol

This project is an implementation of a high-level abstraction of the MQTT protocol using Deno.

It contains a `WebSocket`-like class that can handle MQTT connections. The `MqttSocket` class provides methods for publishing messages, subscribing to topics, and handling authentication. The class also emits events for when the connection is opened, a message is received, a packet is sent or received, an error occurs, the connection is closed, a subscription is made, or an unsubscription is made.

Example usage:

```typescript
import { MqttSocket } from "./mod.ts";

// Create a new MqttSocket instance
const socket = new MqttSocket("mqtt://192.168.3.8:1883");

socket.addEventListener("open", async () => {
    // Subscribe to a topic
    await socket.subscribe([{ topic: "test", qos: 0 }]);

    // Publish a message
    await socket.publish({
        topic: "test",
        payload: "Hello, MQTT!",
        qos: 0,
    });
});

// Receive messages
socket.addEventListener("message", (event) => {
    console.log("Received message:", event.message.payload.text);
});
```

You can import the `MqttSocket` class from the `mod.ts` file. The class provides methods for publishing messages, subscribing to topics, and handling authentication. It also emits events for various actions on the MQTT connection.
