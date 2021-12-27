package com.rarible.blockchain.scanner.ethereum.model

import io.daonomic.rpc.domain.Word
import scalether.domain.Address
import java.time.Instant

data class EthereumLog(

    /**
     * Hash of the transaction.
     * Note that the transaction may be pending, in which case the [blockHash], [blockNumber] and [logIndex] are `null`.
     */
    val transactionHash: String,

    /**
     * Status of the log event. Usually, only [EthereumLogStatus.CONFIRMED] log events impact business calculations.
     */
    val status: EthereumLogStatus,

    /**
     * Address of the smart contract that produced this log event.
     */
    val address: Address,
    /**
     * ID of the log event as defined by Ethereum log (e.g. `keccak256("SomeLogEvent(address,unit256)")`).
     */
    val topic: Word,

    /**
     * Hash of the block inside which this log event was produced, or `null` for pending logs.
     */
    val blockHash: Word? = null,
    /**
     * Number of the block inside which this log event was produced, or `null` for pending logs.
     */
    val blockNumber: Long? = null,
    /**
     * Index of this log event inside the whole block, or `null` for pending logs.
     * This is a native Ethereum value.
     */
    val logIndex: Int? = null,
    /**
     * Secondary index of this log event among all logs produced by `LogEventDescriptor.convert` for the same log
     * with exactly the same [blockNumber], [blockHash], [transactionHash], [logIndex] and [index].
     * The [minorLogIndex] is used to distinguish consequent business events.
     */
    val minorLogIndex: Int,
    /**
     * 0-based index of this log event among logs of the same transaction and having the same topic and coming from the same set of listened addresses.
     * It is different from [logIndex] in that the [logIndex] is per-block but [index] is per-transaction-per-topic-per-set-of-addresses.
     * Note that [logIndex] is a commonly used index (defined in Ethereum spec), whereas the [index] is calculated by our code.
     */
    val index: Int,
    /**
     * Whether this log event should be considered for processing.
     */
    val visible: Boolean = true,

    /**
     * Timestamp of the block (in epoch seconds) to which this log event belongs.
     *
     * This field is nullable until all log events are updated in the database.
     * For new log events it is not null.
    */
    val blockTimestamp: Long? = null,
    val createdAt: Instant = Instant.EPOCH,
    val updatedAt: Instant = Instant.EPOCH

)
