package com.example.events;


/** Small JSON payload shared by the two event topics to keep the lesson short.
 * The topic tells us whether the order was created or stock was reserved.
 * A larger system would usually define separate event types and schema versions.
 */
// A record is a small data holder. Its fields describe the request or event we send.
public record OrderEvent(String orderId, String product, int quantity) {}
