package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.rarible.blockchain.scanner.ethereum.configuration.HyperProperties
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import java.net.URI
import java.util.concurrent.CompletableFuture

class HyperBlockArchiverTest {

    private lateinit var s3Client: S3AsyncClient
    private lateinit var hyperProperties: HyperProperties
    private lateinit var hyperBlockArchiver: HyperBlockArchiver

    @BeforeEach
    fun setup() {
        s3Client = mockk<S3AsyncClient>()
        hyperProperties = HyperProperties(
            enabled = true,
            s3 = URI.create("s3://test-bucket")
        )
        hyperBlockArchiver = HyperBlockArchiver(s3Client, hyperProperties)
    }

    @Test
    fun `should handle missing block`() {
        // Given
        val blockNumber = 12345L
        val exceptionFuture = CompletableFuture<Any>()
        exceptionFuture.completeExceptionally(NoSuchKeyException.builder().build())
        
        every { 
            s3Client.getObject(any<GetObjectRequest>(), any<AsyncResponseTransformer<*, *>>()) 
        } returns exceptionFuture

        // When/Then
        assertThatThrownBy { 
            hyperBlockArchiver.downloadBlock(blockNumber).block() 
        }.isInstanceOf(HyperBlockArchiver.BlockNotFoundException::class.java)
    }

    @Test
    fun `should handle general error during download`() {
        // Given
        val blockNumber = 12345L
        val exceptionFuture = CompletableFuture<Any>()
        exceptionFuture.completeExceptionally(RuntimeException("Network error"))
        
        every { 
            s3Client.getObject(any<GetObjectRequest>(), any<AsyncResponseTransformer<*, *>>()) 
        } returns exceptionFuture

        // When/Then
        assertThatThrownBy { 
            hyperBlockArchiver.downloadBlock(blockNumber).block() 
        }.isInstanceOf(HyperBlockArchiver.BlockProcessingException::class.java)
    }

    @Test
    fun `should format object key correctly`() {
        // This is testing a private method through reflection
        val formatObjectKeyMethod = HyperBlockArchiver::class.java.getDeclaredMethod(
            "formatObjectKey", Long::class.java
        ).apply { isAccessible = true }

        // Testing with different block numbers
        val result1 = formatObjectKeyMethod.invoke(hyperBlockArchiver, 123456L) as String
        val result2 = formatObjectKeyMethod.invoke(hyperBlockArchiver, 7000000L) as String

        assertThat(result1).isEqualTo("blocks/0000000/00123456.bin")
        assertThat(result2).isEqualTo("blocks/0000007/07000000.bin")
    }

    // Additional tests for successful download would require preparing mock LZ4 compressed MessagePack data,
    // which would be complex for a unit test. Consider integration tests for that scenario.
} 