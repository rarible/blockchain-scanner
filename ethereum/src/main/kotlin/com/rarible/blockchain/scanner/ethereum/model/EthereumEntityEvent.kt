package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.ethereum.reduce.CompactEventsReducer
import com.rarible.blockchain.scanner.ethereum.reduce.RevertCompactEventsReducer

abstract class EthereumEntityEvent<T> : Comparable<EthereumEntityEvent<T>> {
    abstract val entityId: String
    abstract val log: EthereumLog
    /**
     * This flag indicates that event is compacted by [CompactEventsReducer]
     * and should be reverted by [RevertCompactEventsReducer]
     */
    abstract val compact: Boolean

    val timestamp: Long get() = log.createdAt.epochSecond

    open fun invert(): T = throw IllegalArgumentException("${this.javaClass} event can't invert")

    fun isConfirmed(): Boolean = log.status == EthereumBlockStatus.CONFIRMED

    override fun compareTo(other: EthereumEntityEvent<T>): Int {
        val o1 = this
        return when (o1.log.status) {
            EthereumBlockStatus.CONFIRMED, EthereumBlockStatus.REVERTED -> {
                require(other.log.status == EthereumBlockStatus.CONFIRMED || other.log.status == EthereumBlockStatus.REVERTED) {
                    "Can't compare $o1 and $other"
                }
                confirmBlockComparator.compare(o1, other)
            }
            EthereumBlockStatus.PENDING, EthereumBlockStatus.INACTIVE, EthereumBlockStatus.DROPPED -> {
                if (other.log.status == EthereumBlockStatus.CONFIRMED) {
                    eventKeyComparator.compare(o1, other)
                } else {
                    require(
                        other.log.status == EthereumBlockStatus.PENDING ||
                                other.log.status == EthereumBlockStatus.INACTIVE ||
                                other.log.status == EthereumBlockStatus.DROPPED
                    ) {
                        "Can't compare $o1 and $other"
                    }
                    pendingBlockComparator.compare(o1, other)
                }
            }
        }
    }

    private companion object {
        val confirmBlockComparator: Comparator<EthereumEntityEvent<*>> = Comparator
            .comparingLong<EthereumEntityEvent<*>> { requireNotNull(it.log.blockNumber) }
            .thenComparingInt { requireNotNull(it.log.logIndex) }
            .thenComparingInt { it.log.minorLogIndex }

        val pendingBlockComparator: Comparator<EthereumEntityEvent<*>> = Comparator
            .comparing<EthereumEntityEvent<*>, String>({ it.log.transactionHash }, { t1, t2 -> t1.compareTo(t2) })
            .thenComparing({ it.log.address.toString() }, { a1, a2 -> a1.compareTo(a2) })
            .thenComparing({ it.log.topic.toString() }, { a1, a2 -> a1.compareTo(a2) })
            .thenComparingInt { it.log.minorLogIndex }

        val eventKeyComparator: Comparator<EthereumEntityEvent<*>> = Comparator
            .comparing<EthereumEntityEvent<*>, String>({ it.log.transactionHash }, { t1, t2 -> t1.compareTo(t2) })
            .thenComparing({ it.log.address.toString() }, { a1, a2 -> a1.compareTo(a2) })
            .thenComparing({ it.log.topic.toString() }, { a1, a2 -> a1.compareTo(a2) })
    }
}
