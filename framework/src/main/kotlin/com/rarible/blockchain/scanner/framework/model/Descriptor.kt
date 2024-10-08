package com.rarible.blockchain.scanner.framework.model

/**
 * Descriptor of LogEvent Subscriber, defines how to store or query Log data from persistent storage
 * or BlockchainClient. For example, such descriptor may define what collection should be used
 * to store specific Log events or may contain properties, required to query filtered data from Blockchain Client.
 */
interface Descriptor<S : LogStorage> {
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
     * This value will be used for naming of topic and consumer group, so better to keep it short and
     * human-readable.
     */
    val groupId: String

    /**
     * Encapsulates knowledge about persistent storage of this kind of logs. Can be shared across different [Descriptor]s.
     * The same object must be used for the same DB structure (table or MongoDB collection).
     */
    val storage: S

    /**
     * Indicates should logs be saved or just published
     */
    fun shouldSaveLogs(): Boolean = true
}
