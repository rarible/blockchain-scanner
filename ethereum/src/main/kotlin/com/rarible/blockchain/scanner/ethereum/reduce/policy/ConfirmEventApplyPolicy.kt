package com.rarible.blockchain.scanner.ethereum.reduce.policy

import com.rarible.blockchain.scanner.ethereum.model.EthereumEntityEvent
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.core.entity.reducer.service.EventApplyPolicy


open class ConfirmEventApplyPolicy<T : EthereumEntityEvent<T>>(
    private val confirmationBlocks: Int
) : EventApplyPolicy<T> {

    override fun reduce(events: List<T>, event: T): List<T> {
        val newEventList = (events + event)
        val lastNotRevertableEvent = newEventList.lastOrNull { current ->
            current.log.status == EthereumBlockStatus.CONFIRMED && isNotReverted(incomeEvent = event, current = current)
        }
        return newEventList
            .filter { current ->
                // we remove all CONFIRMED logs which can't be reverted anymore,
                // except the latest not revertable logs
                // we always must have at least one not revertable log in the list
                current.log.status != EthereumBlockStatus.CONFIRMED ||
                        current == lastNotRevertableEvent ||
                        isReverted(incomeEvent = event, current = current)
            }
    }

    override fun wasApplied(events: List<T>, event: T): Boolean {
        val lastAppliedEvent = events.lastOrNull { it.log.status == EthereumBlockStatus.CONFIRMED }
        return !(lastAppliedEvent == null || lastAppliedEvent < event)
    }

    private fun isReverted(incomeEvent: T, current: T): Boolean {
        return isNotReverted(incomeEvent, current).not()
    }

    private fun isNotReverted(incomeEvent: T, current: T): Boolean {
        val incomeBlockNumber = requireNotNull(incomeEvent.log.blockNumber)
        val currentBlockNumber = requireNotNull(current.log.blockNumber)
        val blockDiff = incomeBlockNumber - currentBlockNumber

        require(blockDiff >= 0) {
            "Block diff between income=$incomeEvent and current=$current can't be negative"
        }
        return blockDiff >= confirmationBlocks
    }
}
