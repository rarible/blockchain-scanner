package com.rarible.blockchain.scanner.ethereum.task

import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import io.mockk.mockk

internal class ReindexBlocksTaskHandlerTest {
    private val reindexHandler = mockk<ReindexHandler>()
    private val reindexBlocksTaskHandler = ReindexBlocksTaskHandler(reindexHandler)


}