package com.rarible.blockchain.scanner.test.handler

import com.rarible.blockchain.scanner.block.BlockStats
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.handler.BlockEventListener
import com.rarible.blockchain.scanner.handler.BlockEventResult
import com.rarible.blockchain.scanner.handler.BlockListenerResult
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import java.util.concurrent.CopyOnWriteArrayList

class TestBlockEventListener : BlockEventListener<TestBlockchainBlock> {

    override val groupId: String = "testGropuId"
    val blockEvents: MutableList<List<BlockEvent<TestBlockchainBlock>>> = CopyOnWriteArrayList()

    override suspend fun process(events: List<BlockEvent<TestBlockchainBlock>>, mode: ScanMode): BlockListenerResult {
        val eventResults = events.map { BlockEventResult(it.number, emptyList(), BlockStats.empty()) }
        return BlockListenerResult(eventResults) { blockEvents += events }
    }
}
