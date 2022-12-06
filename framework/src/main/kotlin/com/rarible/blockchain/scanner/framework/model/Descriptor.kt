package com.rarible.blockchain.scanner.framework.model

/**
 * Descriptor of LogEvent Subscriber, defines how to store or query Log data from persistent storage
 * or BlockchainClient. For example, such descriptor may define what collection should be used
 * to store specific Log events or may contain properties, required to query filtered data from Blockchain Client.
 */
interface Descriptor {
    /**
     * Identifier of descriptor without specific format used for logging and debugging purposes,
     * should be unique across application instance
     */
    val id: String

    /**
     * Alias of descriptor without specific format used for metrics,
     * This value will ge used for naming of metrics, so better to keep it short and
     * human-readable.
     */
    val alias: String?
        get() = null

    /**
     * Name of the group of the descriptors. Subscribers with descriptors of the same group will
     * be handled together and will be published in the same topic (if KafkaPublisher is used).
     * This value will ge used for naming of topic and consumer group, so better to keep it short and
     * human-readable.
     */
    val groupId: String


    /**
     * Type of objects related to this descriptor. Subscriber should produce LogRecords of this type.
     * Due to mongo framework specific of dealing with interfaces, we need to specify such types
     * explicitly in order to avoid unnecessary mongo reflective field checks in queries.
     */
    val entityType: Class<*>

}
