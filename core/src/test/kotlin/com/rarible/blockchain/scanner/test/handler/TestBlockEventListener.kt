package com.rarible.blockchain.scanner.test.handler

import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.handler.BlockEventListener
import java.util.concurrent.CopyOnWriteArrayList

class TestBlockEventListener : BlockEventListener {

    val blockEvents: MutableList<List<BlockEvent>> = CopyOnWriteArrayList()

    override suspend fun process(events: List<BlockEvent>) {
        blockEvents += events
    }
}
