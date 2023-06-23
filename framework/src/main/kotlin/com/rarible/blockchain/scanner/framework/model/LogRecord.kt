package com.rarible.blockchain.scanner.framework.model

/**
 * LogRecord is an entity containing custom data, defined by subscribers and Log data, used for indexed search.
 * Root entity to be stored in persistent storage.
 */
interface LogRecord : Record {

    fun getBlock(): Long?
}
