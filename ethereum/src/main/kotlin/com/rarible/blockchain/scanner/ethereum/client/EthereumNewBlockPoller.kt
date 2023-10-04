package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.model.ReceivedBlock
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.time.delay
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import scalether.core.MonoEthereum
import scalether.domain.response.Block
import java.time.Duration

@ExperimentalCoroutinesApi
class EthereumNewBlockPoller(
    private val ethereum: MonoEthereum,
    private val pollingDelay: Duration
) : EthereumNewBlockSubscriber {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun newHeads(): Flux<ReceivedBlock<Block<Word>>> = channelFlow<ReceivedBlock<Block<Word>>> {
        while (isClosedForSend.not()) {
            val headBlockNumber = ethereum.ethBlockNumber().awaitFirst()
            val head = ethereum.ethGetBlockByNumber(headBlockNumber).awaitFirstOrNull()
            logger.info("Poller get new head block: ${head?.number()}")
            if (head != null) send(ReceivedBlock(head))
            delay(pollingDelay)
        }
    }.asFlux()
}
