package com.rarible.blockchain.scanner.framework.model

/**
 * Descriptor of LogEvent Subscriber, defines how to store or query Log data from persistent storage
 * or BlockchainClient. For example, such descriptor may define what collection should be used
 * to store specific Log events or may contain properties, required to query filtered data from Blockchain Client.
 */
interface Descriptor {
    /**
     * Identifier of descriptor without specific format, should be unique across application instance
     */
    val id: String

    val groupId: String

    /**
     * Topic for publishing (could be a single word, prefixes will be added automatically)
     */
    val topic: String
}