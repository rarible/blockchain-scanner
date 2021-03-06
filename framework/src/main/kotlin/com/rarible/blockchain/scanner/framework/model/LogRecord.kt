package com.rarible.blockchain.scanner.framework.model

/**
 * LogRecord is an entity contain custom data, defined by subscribers and Log data, used for indexed search.
 * Root entity to be stored in persistent storage.
 */
interface LogRecord<L : Log, LR : LogRecord<L, LR>> {

    /**
     * Log data intrinsic for specific Blockchain. Should be set by Scanner framework,
     * subscribers MUST leave it NULL.
     */
    val log: L?

    /**
     * Create copy of this record with new Log data. Since some implementations of Blockchain
     * require to have generated LogRecord before build appropriate Log data, we need to have
     * possibility to set Log after LogRecord created.
     */
    fun withLog(log: L): LR

}