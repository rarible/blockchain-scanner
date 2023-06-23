package com.rarible.blockchain.scanner.framework.model

interface Record {
    /**
     * Returns a key of record required to define Kafka partition for the message with this event.
     * Key should be the same for events, which should be processed in strict order.
     */
    fun getKey(): String
}