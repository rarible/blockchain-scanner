package com.rarible.blockchain.scanner.framework.model

/**
 * LogRecord is an entity containing custom data, defined by subscribers and Log data, used for indexed search.
 * Root entity to be stored in persistent storage.
 */
interface LogRecord {

    /**
     * Log data intrinsic for specific Blockchain. Should be set by Scanner framework,
     * subscribers MUST leave it NULL.
     */
    val log: Log
    /**
     * Returns a key of record required to define Kafka partition for the message with this event.
     * Key should be the same for events, which should be processed in strict order.
     */
    fun getKey(): String

}
