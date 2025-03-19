package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.rarible.blockchain.scanner.ethereum.configuration.HyperArchiveProperties
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
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
    private val archivedBlock2 = javaClass.getResourceAsStream("/hyper/302293.rmp.lz4").use { it!!.readBytes() }

    @Test
    fun `read block - ok`() = runBlocking<Unit> {
        every {
            s3Client.getObject(any<GetObjectRequest>(), any<AsyncResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>())
        } returns CompletableFuture.completedFuture(ResponseBytes.fromByteArrayUnsafe(mockk<GetObjectResponse>(), archivedBlock1))

        val ethBlock = hyperBlockArchiverAdapter.getBlock(BigInteger.ONE)
        assertThat(ethBlock.number()).isEqualTo(BigInteger.valueOf(302292))
    }
}
