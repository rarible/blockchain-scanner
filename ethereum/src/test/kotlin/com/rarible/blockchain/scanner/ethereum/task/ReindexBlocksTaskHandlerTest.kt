package com.rarible.blockchain.scanner.ethereum.task

import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import io.daonomic.rpc.domain.Word
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import scalether.domain.Address

internal class ReindexBlocksTaskHandlerTest {
    private val reindexHandler = mockk<ReindexHandler>()
    private val reindexBlocksTaskHandler = ReindexBlocksTaskHandler(reindexHandler)

    @Test
    fun `should parse task block list param`() {
        val param = "blocks:1,2,3;topics:0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036,0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef;addresses:0xa7a1462a3f067e959a4ddd0630f49be15716341e,0xa7a1462a3f067e959a4ddd0630f49be15716341e"
        val taskParams = ReindexBlocksTaskHandler.TaskParam.parse(param)
        assertThat(taskParams.blocks).isEqualTo(listOf(1L, 2L, 3L))
        assertThat(taskParams.topics).isEqualTo(
            listOf(
                Word.apply("0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036"),
                Word.apply("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            )
        )
        assertThat(taskParams.addresses).isEqualTo(
            listOf(
                Address.apply("0xa7a1462a3f067e959a4ddd0630f49be15716341e"),
                Address.apply("0xa7a1462a3f067e959a4ddd0630f49be15716341e")
            )
        )
        assertThat(taskParams.range).isNull()
        assertThat(taskParams.from).isNull()

        val param1 = "blocks:1,2,;topics:0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036,0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef;addresses:0xa7a1462a3f067e959a4ddd0630f49be15716341e,0xa7a1462a3f067e959a4ddd0630f49be15716341e"
        val taskParams1 = ReindexBlocksTaskHandler.TaskParam.parse(param1)
        assertThat(taskParams1.blocks).isEqualTo(listOf(1L, 2L))

        val param2 = "blocks:1,;topics:0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036,0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef;addresses:0xa7a1462a3f067e959a4ddd0630f49be15716341e,0xa7a1462a3f067e959a4ddd0630f49be15716341e"
        val taskParams2 = ReindexBlocksTaskHandler.TaskParam.parse(param2)
        assertThat(taskParams2.blocks).isEqualTo(listOf(1L))
    }

    @Test
    fun `should parse task block from param`() {
        val param = "blocks:100-;topics:0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036,0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef;addresses:0xa7a1462a3f067e959a4ddd0630f49be15716341e,0xa7a1462a3f067e959a4ddd0630f49be15716341e"
        val taskParams = ReindexBlocksTaskHandler.TaskParam.parse(param)
        assertThat(taskParams.from).isEqualTo(100L)
        assertThat(taskParams.range).isNull()
        assertThat(taskParams.blocks).isNull()
        assertThat(taskParams.topics).isEqualTo(
            listOf(
                Word.apply("0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036"),
                Word.apply("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            )
        )
        assertThat(taskParams.addresses).isEqualTo(
            listOf(
                Address.apply("0xa7a1462a3f067e959a4ddd0630f49be15716341e"),
                Address.apply("0xa7a1462a3f067e959a4ddd0630f49be15716341e")
            )
        )
    }

    @Test
    fun `should parse task block range param`() {
        val param = "blocks:100-1001;topics:0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036,0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef;addresses:0xa7a1462a3f067e959a4ddd0630f49be15716341e,0xa7a1462a3f067e959a4ddd0630f49be15716341e"
        val taskParams = ReindexBlocksTaskHandler.TaskParam.parse(param)
        assertThat(taskParams.range).isEqualTo(LongRange(100, 1001))
        assertThat(taskParams.blocks).isNull()
        assertThat(taskParams.topics).isEqualTo(
            listOf(
                Word.apply("0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036"),
                Word.apply("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            )
        )
        assertThat(taskParams.addresses).isEqualTo(
            listOf(
                Address.apply("0xa7a1462a3f067e959a4ddd0630f49be15716341e"),
                Address.apply("0xa7a1462a3f067e959a4ddd0630f49be15716341e")
            )
        )
    }

    @Test
    fun `should parse task block from param without addresses`() {
        val param = "blocks:100-1001;topics:0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036,0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        val taskParams = ReindexBlocksTaskHandler.TaskParam.parse(param)
        assertThat(taskParams.range).isEqualTo(LongRange(100, 1001))
        assertThat(taskParams.blocks).isNull()
        assertThat(taskParams.topics).isEqualTo(
            listOf(
                Word.apply("0x750d13f39f16526306cffdefb909852b055c2ea79ee21d21b36402eddaae7036"),
                Word.apply("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            )
        )
        assertThat(taskParams.addresses).hasSize(0)
    }
}