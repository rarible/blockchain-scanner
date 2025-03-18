package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.rarible.blockchain.scanner.ethereum.configuration.HyperProperties
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
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
    private val hyperProperties = HyperProperties()
    private val hyperBlockArchiver = HyperBlockArchiver(s3Client, hyperProperties)

    @Test
    fun `read block - ok`() = runBlocking<Unit> {
        val archivedBlock = javaClass.getResourceAsStream("/hyper/block-5000.rmp.lz4").use {
            it!!.readBytes()
        }
        every {
            s3Client.getObject(any<GetObjectRequest>(), any<AsyncResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>>())
        } returns CompletableFuture.completedFuture(ResponseBytes.fromByteArrayUnsafe(mockk<GetObjectResponse>(), archivedBlock))

        val block = hyperBlockArchiver.downloadBlock(BigInteger.ONE)
    }
}
