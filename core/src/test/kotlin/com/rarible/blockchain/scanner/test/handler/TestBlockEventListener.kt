package com.rarible.blockchain.scanner.test.handler

import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.handler.BlockEventListener
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import java.util.concurrent.CopyOnWriteArrayList

class TestBlockEventListener : BlockEventListener<TestBlockchainBlock> {

    val blockEvents: MutableList<List<BlockEvent<TestBlockchainBlock>>> = CopyOnWriteArrayList()

    override suspend fun process(events: List<BlockEvent<TestBlockchainBlock>>): BlockEventListener.Result {
        return BlockEventListener.Result(emptyMap()) { blockEvents += events }
    }
}
