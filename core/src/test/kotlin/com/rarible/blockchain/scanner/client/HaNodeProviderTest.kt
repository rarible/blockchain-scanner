package com.rarible.blockchain.scanner.client

import com.rarible.core.test.wait.Wait.waitAssert
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

class HaNodeProviderTest {

    @Test
    fun `should return first available local node`() = runBlocking<Unit> {
        // Given
        val provider = HaNodeProvider(
            localNodeConfigs = listOf("l1", "l2", "l3"),
            externalNodeConfigs = listOf("en1", "en2"),
            monitoringInterval = Duration.ofMillis(100),
            connect = { it },
            isHealthy = { c, _ -> c == "l2" }
        )

        // Then
        assertThat(provider.getNode()).isEqualTo("l2")
    }

    @Test
    fun `should return first available external node`() = runBlocking<Unit> {
        // Given
        val provider = HaNodeProvider(
            localNodeConfigs = listOf("l1", "l2", "l3"),
            externalNodeConfigs = listOf("en1", "en2"),
            monitoringInterval = Duration.ofMillis(100),
            connect = { it },
            isHealthy = { c, _ -> c == "en2" }
        )

        // Then
        assertThat(provider.getNode()).isEqualTo("en2")
    }

    @Test
    fun `should failover to next available local node`() = runBlocking {
        // Given
        val availableNodes = mutableSetOf("l1")
        val provider = HaNodeProvider(
            localNodeConfigs = listOf("l1", "l2", "l3"),
            externalNodeConfigs = listOf("en1", "en2"),
            monitoringInterval = Duration.ofMillis(100),
            connect = { it },
            isHealthy = { c, _ -> c in availableNodes }
        )

        // Then
        assertThat(provider.getNode()).isEqualTo("l1")

        // When
        availableNodes.clear()
        availableNodes.add("l2")

        // Then
        waitAssert { assertThat(provider.getNode()).isEqualTo("l2") }
    }

    @Test
    fun `should failover to available external node`() = runBlocking {
        // Given
        val availableNodes = mutableSetOf("l1", "en1")
        val provider = HaNodeProvider(
            localNodeConfigs = listOf("l1", "l2", "l3"),
            externalNodeConfigs = listOf("en1", "en2"),
            monitoringInterval = Duration.ofMillis(100),
            connect = { it },
            isHealthy = { c, _ -> c in availableNodes }
        )

        // Then
        assertThat(provider.getNode()).isEqualTo("l1")

        // When
        availableNodes.clear()
        availableNodes.add("en2")

        // Then
        waitAssert { assertThat(provider.getNode()).isEqualTo("en2") }
    }

    @Test
    fun `should monitor and select an available node`() = runBlocking {
        // Given
        val availableNodes = mutableSetOf<String>()
        val provider = HaNodeProvider(
            localNodeConfigs = listOf("l1", "l2", "l3"),
            externalNodeConfigs = listOf("en1", "en2"),
            monitoringInterval = Duration.ofMillis(100),
            connect = { it },
            isHealthy = { c, _ -> c in availableNodes }
        )

        // When
        assertThrows<NodesUnavailableException> { provider.getNode() }

        // Then
        availableNodes.add("l2")
        waitAssert { assertThat(provider.getNode()).isEqualTo("l2") }
    }

    @Test
    fun `should prefer a local node over an external node`() = runBlocking {
        // Given
        val availableNodes = mutableSetOf<String>("en1")
        val provider = HaNodeProvider(
            localNodeConfigs = listOf("l1", "l2", "l3"),
            externalNodeConfigs = listOf("en1", "en2"),
            monitoringInterval = Duration.ofMillis(100),
            connect = { it },
            isHealthy = { c, _ -> c in availableNodes }
        )

        // Then
        assertThat(provider.getNode()).isEqualTo("en1")

        // When
        availableNodes.add("l2")

        waitAssert { assertThat(provider.getNode()).isEqualTo("l2") }
    }

    @Test
    fun `should retain last available node`() = runBlocking {
        // Given
        val availableNodes = mutableSetOf("en1")
        val provider = HaNodeProvider(
            localNodeConfigs = listOf("l1", "l2", "l3"),
            externalNodeConfigs = listOf("en1", "en2"),
            monitoringInterval = Duration.ofMillis(5),
            connect = { it },
            isHealthy = { c, _ -> c in availableNodes }
        )

        // Then
        assertThat(provider.getNode()).isEqualTo("en1")

        // When
        availableNodes.clear()

        // Then
        waitAssert { assertThat(provider.getNode()).isEqualTo("en1") }
    }
}
