package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.rarible.blockchain.scanner.ethereum.configuration.HyperArchiveProperties
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import scalether.domain.Address
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import java.math.BigInteger
import java.util.concurrent.CompletableFuture

class HyperBlockArchiverTest {
    private val s3Client = mockk<S3AsyncClient>()
    private val hyperProperties = HyperArchiveProperties()
    private val hyperBlockArchiver = HyperBlockArchiver(s3Client, hyperProperties)
    private val cachedHyperBlockArchiver = CachedHyperBlockArchiver(hyperBlockArchiver, hyperProperties)
    private val hyperBlockArchiverAdapter = HyperBlockArchiverAdapter(cachedHyperBlockArchiver)

    private val archivedBlock1 = javaClass.getResourceAsStream("/hyper/302292.rmp.lz4").use { it!!.readBytes() }
    private val archivedBlock3 = javaClass.getResourceAsStream("/hyper/302788.rmp.lz4").use { it!!.readBytes() }
    private val archivedBlockNoChainId = javaClass.getResourceAsStream("/hyper/5732.rmp.lz4").use { it!!.readBytes() }

    @Test
    fun `read big block - ok`() = runBlocking<Unit> {
        every {
            s3Client.getObject(any<GetObjectRequest>(), any<AsyncResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>())
        } returns CompletableFuture.completedFuture(ResponseBytes.fromByteArrayUnsafe(mockk<GetObjectResponse>(), archivedBlock1))

        val ethBlock = hyperBlockArchiverAdapter.getBlock(BigInteger.ONE)
        assertThat(ethBlock.number()).isEqualTo(BigInteger.valueOf(302292))
    }

    @ParameterizedTest
    @CsvSource(
        "1, '0/0/1.rmp.lz4'",
        "1000, '0/0/1000.rmp.lz4'",
        "1001, '0/1000/1001.rmp.lz4'",
    )
    fun `block number is correctly mapped to S3 key`(blockNumber: Int, expectedKey: String) = runBlocking<Unit> {
        every {
            s3Client.getObject(any<GetObjectRequest>(), any<AsyncResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>())
        } returns CompletableFuture.completedFuture(ResponseBytes.fromByteArrayUnsafe(mockk<GetObjectResponse>(), archivedBlock1))

        val ethBlock = hyperBlockArchiverAdapter.getBlock(blockNumber.toBigInteger())
        assertThat(ethBlock.number()).isEqualTo(BigInteger.valueOf(302292))
        verify {
            s3Client.getObject(match<GetObjectRequest> { it.key() == expectedKey }, any<AsyncResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>())
        }
    }

    @Test
    fun `read block - ok`() = runBlocking<Unit> {
        every {
            s3Client.getObject(any<GetObjectRequest>(), any<AsyncResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>())
        } returns CompletableFuture.completedFuture(ResponseBytes.fromByteArrayUnsafe(mockk<GetObjectResponse>(), archivedBlock3))

        val ethBlock = hyperBlockArchiverAdapter.getBlock(BigInteger.ONE)

        // Verify block header attributes
        assertThat(ethBlock.number()).isEqualTo(BigInteger.valueOf(302788))
        assertThat(ethBlock.hash().toString()).isEqualTo("0x43238049e81778ff2a4c2801bab54fc8af5ce5ce160f192dfff13a2812071d63")
        assertThat(ethBlock.parentHash().toString()).isEqualTo("0xd93c77d4885b9515515c8b9d5aa57b864406452ccfa63bf57d18fcc7f84f562a")
        assertThat(ethBlock.timestamp()).isEqualTo(BigInteger.valueOf(1740435900))
        assertThat(ethBlock.gasLimit()).isEqualTo(BigInteger.valueOf(30000000))
        assertThat(ethBlock.gasUsed()).isEqualTo(BigInteger.valueOf(3268471))

        // Verify transactions count
        assertThat(ethBlock.transactions().size()).isEqualTo(23)

        // Verify first transaction
        val firstTx = ethBlock.transactions().apply(0)
        assertThat(firstTx.hash().toString()).isEqualTo("0x0324bf0e12e05b10714367d2ba917d2b90ad8775d9deb8dd2684a2b923522dc6")
        assertThat(firstTx.nonce()).isEqualTo(BigInteger.valueOf(7))
        assertThat(firstTx.gas()).isEqualTo(BigInteger.valueOf(700000))
        assertThat(firstTx.gasPrice()).isEqualTo(BigInteger.valueOf(2500000000000L))
        assertThat(firstTx.to().toString().lowercase()).isEqualTo("0x4adb7665c72ccdad25ed5b0bd87c34e4ee9da3c4")
        assertThat(firstTx.value()).isEqualTo(BigInteger.valueOf(1000000000000000000L)) // 1 ETH
        assertThat(firstTx.input().toString()).isEqualTo("0xa0712d680000000000000000000000000000000000000000000000000000000000000001")

        // Verify second transaction
        val secondTx = ethBlock.transactions().apply(1)
        assertThat(secondTx.hash().toString()).isEqualTo("0xf7235df8d6382684e86308688f032d9549ac180c98bdadf56ce657672e83bcbb")
        assertThat(secondTx.nonce()).isEqualTo(BigInteger.valueOf(5))
        assertThat(secondTx.gas()).isEqualTo(BigInteger.valueOf(964947))
        assertThat(secondTx.to().toString().lowercase()).isEqualTo("0x0000000000000068f116a894984e2db1123eb395")

        // Verify legacy transaction (fourth transaction)
        val legacyTx = ethBlock.transactions().apply(3)
        assertThat(legacyTx.hash().toString()).isEqualTo("0xee2b1d14f9d80b309e6148e76c0ee63b12d2af180e437f0dbdc2c6be582d8a99")
        assertThat(legacyTx.nonce()).isEqualTo(BigInteger.valueOf(35))
        assertThat(legacyTx.gasPrice()).isEqualTo(BigInteger.valueOf(40000000000L))
        assertThat(legacyTx.to().toString().lowercase()).isEqualTo("0x4adb7665c72ccdad25ed5b0bd87c34e4ee9da3c4")
        assertThat(legacyTx.value()).isEqualTo(BigInteger.valueOf(2000000000000000000L)) // 2 ETH
        assertThat(legacyTx.input().toString()).isEqualTo("0xa0712d680000000000000000000000000000000000000000000000000000000000000002")
    }

    @Test
    fun `read block logs - ok`() = runBlocking<Unit> {
        every {
            s3Client.getObject(any<GetObjectRequest>(), any<AsyncResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>())
        } returns CompletableFuture.completedFuture(ResponseBytes.fromByteArrayUnsafe(mockk<GetObjectResponse>(), archivedBlock3))

        val logs = hyperBlockArchiverAdapter.getLogsByBlockRange(BigInteger.ONE, BigInteger.ONE)

        // Verify total number of logs
        assertThat(logs.size).isEqualTo(16)

        // Verify log index continuity - should start at 0 and increment consecutively
        val logIndices = logs.map { it.logIndex() }
        assertThat(logIndices).containsExactly(
            BigInteger.valueOf(0), BigInteger.valueOf(1), BigInteger.valueOf(2),
            BigInteger.valueOf(3), BigInteger.valueOf(4), BigInteger.valueOf(5),
            BigInteger.valueOf(6), BigInteger.valueOf(7), BigInteger.valueOf(8),
            BigInteger.valueOf(9), BigInteger.valueOf(10), BigInteger.valueOf(11),
            BigInteger.valueOf(12), BigInteger.valueOf(13), BigInteger.valueOf(14),
            BigInteger.valueOf(15)
        )

        // Group logs by transaction index
        val logsByTxIndex = logs.groupBy { it.transactionIndex() }

        // Verify logs from transaction with 5 logs (receipt[1])
        val txWith5Logs = logsByTxIndex.entries.find { it.value.size == 5 }
        assertThat(txWith5Logs).isNotNull
        assertThat(txWith5Logs!!.key).isEqualTo(BigInteger.valueOf(1))

        // Verify first log of transaction with 5 logs
        val firstLog = txWith5Logs.value.first()
        assertThat(firstLog.address().toString().lowercase()).isEqualTo("0xcc3d60ff11a268606c6a57bd6db74b4208f1d30c")
        assertThat(firstLog.topics().size()).isEqualTo(4)
        assertThat(firstLog.topics().apply(0).toString()).isEqualTo("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
        assertThat(firstLog.removed()).isFalse()

        // Find transaction that has 1 log for ERC-721 transfer
        val txWithERC721Transfer = logsByTxIndex.entries
            .filter { it.value.size == 1 }
            .find { entry ->
                val log = entry.value.first()
                log.address().toString().lowercase() == "0x4adb7665c72ccdad25ed5b0bd87c34e4ee9da3c4" &&
                log.topics().apply(0).toString() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            }
        assertThat(txWithERC721Transfer).isNotNull

        // Find transaction with most logs (should be 4)
        val txWithMostLogs = logsByTxIndex.entries.maxByOrNull { it.value.size }
        assertThat(txWithMostLogs).isNotNull
        assertThat(txWithMostLogs!!.value.size).isEqualTo(5)

        // Verify the last log
        val lastLog = logs.last()
        assertThat(lastLog.logIndex()).isEqualTo(BigInteger.valueOf(15))
        assertThat(lastLog.address().toString().lowercase()).isEqualTo("0x5555555555555555555555555555555555555555")
    }

    @Test
    fun `can read block with transaction without chain ID`() = runBlocking<Unit> {
        every {
            s3Client.getObject(any<GetObjectRequest>(), any<AsyncResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>())
        } returns CompletableFuture.completedFuture(ResponseBytes.fromByteArrayUnsafe(mockk<GetObjectResponse>(), archivedBlockNoChainId))

        val ethBlock = hyperBlockArchiverAdapter.getBlock(BigInteger.ONE)

        // Verify block header attributes
        assertThat(ethBlock.number()).isEqualTo(BigInteger.valueOf(0x1664))
        assertThat(ethBlock.hash().toString()).isEqualTo("0xdf907704273c1307016bedbee720cdb5bca6a7b0c4edb0f1dbecf59c489d5cc0")
        assertThat(ethBlock.parentHash().toString()).isEqualTo("0xf760e7d1949096f6b19479a11d06ba2ac33aac9509d227e0516d6474c63d628f")
        assertThat(ethBlock.timestamp()).isEqualTo(BigInteger.valueOf(0x0000000067b42b86L))
        assertThat(ethBlock.gasLimit()).isEqualTo(BigInteger.valueOf(0x00000000001e8480L))
        assertThat(ethBlock.gasUsed()).isEqualTo(BigInteger.valueOf(0x0000000000015c31L))

        // Verify transactions count
        assertThat(ethBlock.transactions().size()).isEqualTo(2)

        // Verify first transaction
        val firstTx = ethBlock.transactions().apply(0)
        assertThat(firstTx.hash().toString()).isEqualTo("0x2222222222222222222222222222222222222222222222222222222222222222")
        assertThat(firstTx.nonce()).isEqualTo(BigInteger.valueOf(0))
        assertThat(firstTx.gas()).isEqualTo(BigInteger.valueOf(0x00000000000186a0))
        assertThat(firstTx.gasPrice()).isEqualTo(BigInteger.valueOf(0x0000000000000000000000174876e800L))
        assertThat(firstTx.to()).isEqualTo(Address.ZERO())
        assertThat(firstTx.value()).isEqualTo(BigInteger.valueOf(0))
        assertThat(firstTx.input().toString()).isEqualTo("0x604580600e600039806000f350fe7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe03601600081602082378035828234f58015156039578182fd5b8082525050506014600cf3")

        // Verify second transaction
        val secondTx = ethBlock.transactions().apply(1)
        assertThat(secondTx.hash().toString()).isEqualTo("0x16e28a7275632633d6a0a0f9f3bfb0b9d964d97b5a5130d4f619f59f4ba5603a")
        assertThat(secondTx.nonce()).isEqualTo(BigInteger.valueOf(1))
        assertThat(secondTx.gas()).isEqualTo(BigInteger.valueOf(0x0000000000005208L))
        assertThat(secondTx.to().toString().lowercase()).isEqualTo("0x3fab184622dc19b6109349b94811493bf2a45362")
    }
}
