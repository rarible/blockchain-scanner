package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.FlowChainId
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlowAccessApiClientManagerTest {

    @ParameterizedTest
    @MethodSource("sporkByBlockHeightTestArguments")
    internal fun sporkByBlockHeightTest(blockHeight: Long, expectedSpork: FlowAccessApiClientManager.Spork) {
        val actualSpork = FlowAccessApiClientManager.async(blockHeight, FlowChainId.MAINNET)
        Assertions.assertEquals(expectedSpork, actualSpork)
    }

    @ParameterizedTest
    @MethodSource("sporkByBlockHashTestArguments")
    internal fun sporkByBlockHashTest(blockHash: String, expectedSpork: FlowAccessApiClientManager.Spork) {
        val actualSpork = runBlocking { FlowAccessApiClientManager.async(blockHash, FlowChainId.MAINNET) }
        Assertions.assertEquals(expectedSpork, actualSpork)
    }

    @ParameterizedTest
    @MethodSource("sporkByBlockHeightRangeArguments")
    internal fun sporkByBlockHeightRangeTest(range: LongRange, sporks: List<FlowAccessApiClientManager.Spork>) {
        val actualSporks = FlowAccessApiClientManager.async(range, FlowChainId.MAINNET)
        Assertions.assertArrayEquals(sporks.toTypedArray(), actualSporks.toTypedArray())
    }

    private fun sporkByBlockHeightRangeArguments(): Stream<Arguments> = Stream.of(
        Arguments.of(
            14019714L..14052280L,
            listOf(FlowAccessApiClientManager.Spork(
                from = 13950742L,
                to = 14892103L,
                nodeUrl = "access-001.mainnet8.nodes.onflow.org"
            )),
        ),
        Arguments.of(
            14891903L..14892404L,
            listOf(
                FlowAccessApiClientManager.Spork(
                    from = 13950742L,
                    to = 14892103L,
                    nodeUrl = "access-001.mainnet8.nodes.onflow.org"
                ),
                FlowAccessApiClientManager.Spork(
                    from = 14892104L,
                    to = 15791890L,
                    nodeUrl = "access-001.mainnet9.nodes.onflow.org"
                )
            )
        )
    )

    private fun sporkByBlockHashTestArguments(): Stream<Arguments> = Stream.of(
        Arguments.of(
            "870215258aedaac0066f1ed0d92da301a67ebacb4358b51b5ab7795849d2d86d",
            FlowAccessApiClientManager.Spork(
                from = 7601063L,
                to = 8742958L,
                nodeUrl = "access-001.mainnet1.nodes.onflow.org"
            )
        ),
        Arguments.of(
            "4d6b4294ba059577b77d6c053ce94227b74a986823feac77c696caba0c2dd4fe",
            FlowAccessApiClientManager.Spork(
                from = 13950742L,
                to = 14892103L,
                nodeUrl = "access-001.mainnet8.nodes.onflow.org"
            )
        )
    )

    private fun sporkByBlockHeightTestArguments(): Stream<Arguments> = Stream.of(
        Arguments.of(
            9737133L,
            FlowAccessApiClientManager.Spork(
                from = 9737133L,
                to = 9992019L,
                nodeUrl = "access-001.mainnet3.nodes.onflow.org"
            )
        ),
        Arguments.of(
            13950241L,
            FlowAccessApiClientManager.Spork(
                from = 13404174L,
                to = 13950741L,
                nodeUrl = "access-001.mainnet7.nodes.onflow.org"
            )
        )
    )
}
