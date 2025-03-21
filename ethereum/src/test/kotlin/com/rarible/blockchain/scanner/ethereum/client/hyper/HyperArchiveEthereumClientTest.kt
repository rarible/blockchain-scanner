package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.test.data.ethBlock
import com.rarible.blockchain.scanner.ethereum.test.data.ethLog
import com.rarible.blockchain.scanner.ethereum.test.data.ethTransaction
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlockHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import scala.jdk.javaapi.CollectionConverters
import scalether.domain.response.Block
import java.math.BigInteger

class HyperArchiveEthereumClientTest {

    private val hyperBlockArchiverAdapter = mockk<HyperBlockArchiverAdapter>()
    private val properties = mockk<EthereumScannerProperties>()
    private val scanProperties = mockk<ScanProperties>()
    private val logRepository = mockk<EthereumLogRepository>()
    private val blockRepository = mockk<BlockRepository>()
    private lateinit var client: HyperArchiveEthereumClient

    @BeforeEach
    fun setUp() {
        every { properties.scan } returns scanProperties
        every { scanProperties.firstAvailableBlock } returns 1L

        coEvery { hyperBlockArchiverAdapter.getBlock(any()) } answers {
            val blockNumber = firstArg<BigInteger>()
            ethBlock(blockNumber.toInt())
        }

        coEvery { hyperBlockArchiverAdapter.getLogsByBlockRange(any(), any()) } returns emptyList()

        client = HyperArchiveEthereumClient(hyperBlockArchiverAdapter, blockRepository, properties)
    }

    @Test
    fun `should get block by number`() = runBlocking<Unit> {
        val blockNumber = 12345L
        val block = ethBlock(blockNumber.toInt())

        coEvery { hyperBlockArchiverAdapter.getBlock(BigInteger.valueOf(blockNumber)) } returns block

        val result = client.getBlock(blockNumber)

        assertThat(result).isNotNull
        assertThat(result.number).isEqualTo(blockNumber)
        assertThat(result.ethBlock).isEqualTo(block)
    }

    @Test
    fun `should get multiple blocks`() = runBlocking<Unit> {
        val blockNumbers = listOf(100L, 200L, 300L)
        val blocks = blockNumbers.map {
            val block = ethBlock(it.toInt())
            coEvery { hyperBlockArchiverAdapter.getBlock(BigInteger.valueOf(it)) } returns block
            block
        }

        val results = client.getBlocks(blockNumbers)

        assertThat(results).hasSize(3)
        blockNumbers.forEachIndexed { index, number ->
            assertThat(results[index].number).isEqualTo(number)
            assertThat(results[index].ethBlock).isEqualTo(blocks[index])
        }
    }

    @Test
    fun `should get first available block`() = runBlocking<Unit> {
        val blockNumber = 1L
        val block = ethBlock(blockNumber.toInt())
        coEvery { hyperBlockArchiverAdapter.getBlock(BigInteger.valueOf(blockNumber)) } returns block

        val result = client.getFirstAvailableBlock()

        assertThat(result).isNotNull
        assertThat(result.number).isEqualTo(blockNumber)
    }

    @Test
    fun `getLastBlockNumber from block repository`() = runBlocking<Unit> {
        coEvery { blockRepository.getLastBlock() } returns mockk {
            every { id } returns 123L
        }
        val latest = client.getLastBlockNumber()
        assertThat(latest).isEqualTo(123L)
    }

    @Test
    fun `should get block logs`() = runBlocking<Unit> {
        val topic = randomWord()
        val contractAddress = randomAddress()
        val descriptor = EthereumDescriptor(
            ethTopic = topic,
            groupId = "test-group",
            contracts = listOf(contractAddress),
            storage = logRepository
        )

        val block1Number = 101
        val block1Hash = randomBlockHash()
        val block2Number = 102
        val block2Hash = randomBlockHash()

        val tx1Hash = randomWord()
        val tx2Hash = randomWord()

        val tx1 = ethTransaction(tx1Hash, block1Hash, BigInteger.valueOf(block1Number.toLong()))
        val tx2 = ethTransaction(tx2Hash, block2Hash, BigInteger.valueOf(block2Number.toLong()))

        val baseBlock1 = ethBlock(block1Number, block1Hash)
        val block1WithTx = Block(
            baseBlock1.number(),
            baseBlock1.hash(),
            baseBlock1.parentHash(),
            baseBlock1.nonce(),
            baseBlock1.sha3Uncles(),
            baseBlock1.logsBloom(),
            baseBlock1.transactionsRoot(),
            baseBlock1.stateRoot(),
            baseBlock1.miner(),
            baseBlock1.difficulty(),
            baseBlock1.totalDifficulty(),
            baseBlock1.extraData(),
            baseBlock1.size(),
            baseBlock1.gasLimit(),
            baseBlock1.gasUsed(),
            CollectionConverters.asScala(listOf(tx1)).toList(),
            baseBlock1.timestamp()
        )

        val baseBlock2 = ethBlock(block2Number, block2Hash)
        val block2WithTx = Block(
            baseBlock2.number(),
            baseBlock2.hash(),
            baseBlock2.parentHash(),
            baseBlock2.nonce(),
            baseBlock2.sha3Uncles(),
            baseBlock2.logsBloom(),
            baseBlock2.transactionsRoot(),
            baseBlock2.stateRoot(),
            baseBlock2.miner(),
            baseBlock2.difficulty(),
            baseBlock2.totalDifficulty(),
            baseBlock2.extraData(),
            baseBlock2.size(),
            baseBlock2.gasLimit(),
            baseBlock2.gasUsed(),
            CollectionConverters.asScala(listOf(tx2)).toList(),
            baseBlock2.timestamp()
        )

        val log1 = ethLog(tx1Hash, topic, contractAddress, 0, block1Hash, baseBlock1.number())
        val log2 = ethLog(tx2Hash, topic, contractAddress, 1, block2Hash, baseBlock2.number())

        val ethBlocks = listOf(
            EthereumBlockchainBlock(block1WithTx),
            EthereumBlockchainBlock(block2WithTx)
        )

        coEvery { hyperBlockArchiverAdapter.getBlock(BigInteger.valueOf(block1Number.toLong())) } returns block1WithTx
        coEvery { hyperBlockArchiverAdapter.getBlock(BigInteger.valueOf(block2Number.toLong())) } returns block2WithTx

        coEvery {
            hyperBlockArchiverAdapter.getLogsByBlockRange(
                fromBlock = BigInteger.valueOf(block1Number.toLong()),
                toBlock = BigInteger.valueOf(block2Number.toLong())
            )
        } returns listOf(log1, log2)

        val result = client.getBlockLogs(descriptor, ethBlocks, true).toList()

        assertThat(result).hasSize(2)

        assertThat(result[0].block.number).isEqualTo(block1Number.toLong())
        assertThat(result[0].logs).hasSize(1)
        assertThat(result[0].logs[0].ethLog.transactionHash()).isEqualTo(tx1Hash)

        assertThat(result[1].block.number).isEqualTo(block2Number.toLong())
        assertThat(result[1].logs).hasSize(1)
        assertThat(result[1].logs[0].ethLog.transactionHash()).isEqualTo(tx2Hash)
    }

    @Test
    fun `should filter logs by topic and contract address`() = runBlocking<Unit> {
        val topic = randomWord()
        val relevantAddress = randomAddress()
        val irrelevantAddress = randomAddress()
        val descriptor = EthereumDescriptor(
            ethTopic = topic,
            groupId = "test-group",
            contracts = listOf(relevantAddress),
            storage = logRepository
        )

        val blockNumber = 100
        val blockHash = randomBlockHash()

        val tx1Hash = randomWord()
        val tx2Hash = randomWord()
        val tx3Hash = randomWord()

        val tx1 = ethTransaction(tx1Hash, blockHash, BigInteger.valueOf(blockNumber.toLong()))
        val tx2 = ethTransaction(tx2Hash, blockHash, BigInteger.valueOf(blockNumber.toLong()))
        val tx3 = ethTransaction(tx3Hash, blockHash, BigInteger.valueOf(blockNumber.toLong()))

        val baseBlock = ethBlock(blockNumber, blockHash)
        val blockWithTx = Block(
            baseBlock.number(),
            baseBlock.hash(),
            baseBlock.parentHash(),
            baseBlock.nonce(),
            baseBlock.sha3Uncles(),
            baseBlock.logsBloom(),
            baseBlock.transactionsRoot(),
            baseBlock.stateRoot(),
            baseBlock.miner(),
            baseBlock.difficulty(),
            baseBlock.totalDifficulty(),
            baseBlock.extraData(),
            baseBlock.size(),
            baseBlock.gasLimit(),
            baseBlock.gasUsed(),
            CollectionConverters.asScala(listOf(tx1, tx2, tx3)).toList(),
            baseBlock.timestamp()
        )

        val relevantLog = ethLog(tx1Hash, topic, relevantAddress, 0, blockHash, baseBlock.number())
        val logWithWrongTopic = ethLog(tx2Hash, randomWord(), relevantAddress, 1, blockHash, baseBlock.number())
        val logWithWrongAddress = ethLog(tx3Hash, topic, irrelevantAddress, 2, blockHash, baseBlock.number())

        val ethBlock = EthereumBlockchainBlock(blockWithTx)

        coEvery { hyperBlockArchiverAdapter.getBlock(BigInteger.valueOf(blockNumber.toLong())) } returns blockWithTx

        coEvery {
            hyperBlockArchiverAdapter.getLogsByBlockRange(
                fromBlock = BigInteger.valueOf(blockNumber.toLong()),
                toBlock = BigInteger.valueOf(blockNumber.toLong())
            )
        } returns listOf(relevantLog, logWithWrongTopic, logWithWrongAddress)

        val result = client.getBlockLogs(descriptor, listOf(ethBlock), true).toList()

        assertThat(result).hasSize(1)
        assertThat(result[0].logs).hasSize(1)
        assertThat(result[0].logs[0].ethLog.transactionHash()).isEqualTo(tx1Hash)
        assertThat(result[0].logs[0].ethLog.address()).isEqualTo(relevantAddress)
        assertThat(result[0].logs[0].ethLog.topics().head()).isEqualTo(topic)
    }

    @Test
    fun `should throw error for unstable blocks`() {
        val block = EthereumBlockchainBlock(ethBlock(100))
        val descriptor = EthereumDescriptor(
            ethTopic = randomWord(),
            groupId = "test-group",
            contracts = emptyList(),
            storage = logRepository
        )

        assertThrows<IllegalArgumentException> {
            runBlocking {
                client.getBlockLogs(descriptor, listOf(block), false).toList()
            }
        }
    }
}
