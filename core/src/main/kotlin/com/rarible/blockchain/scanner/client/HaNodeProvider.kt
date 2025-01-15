package com.rarible.blockchain.scanner.client

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class NodesUnavailableException : Exception()

private data class NodeReference<C, H>(val config: C, val handle: H)

interface NodeProvider<CONFIG, NODE> {
    suspend fun getNode(): NODE
}

class HaNodeProvider<CONFIG, NODE>(
    private val localNodeConfigs: List<CONFIG>,
    private val externalNodeConfigs: List<CONFIG> = emptyList(),
    private val monitoringInterval: Duration,
    private val connect: (CONFIG) -> NODE,
    private val isHealthy: suspend (CONFIG, NODE) -> Boolean
) : AutoCloseable, NodeProvider<CONFIG, NODE> {

    private val logger = LoggerFactory.getLogger(HaNodeProvider::class.java)
    private val currentNode = AtomicReference<NodeReference<CONFIG, NODE>>()
    private val monitoringThread = MonitoringThread()
    private val mutex = Mutex()

    init {
        logger.info("Initialized with local nodes: $localNodeConfigs, external nodes: $externalNodeConfigs")
        monitoringThread.start()
    }

    override suspend fun getNode(): NODE {
        return currentNode.get()?.handle ?: findAvailableNode()?.handle ?: throw NodesUnavailableException()
    }

    private suspend fun findAvailableNode(): NodeReference<CONFIG, NODE>? {
        return mutex.withLock {
            val cachedNode = currentNode.get()
            if (cachedNode == null || !isNodeHealthy(cachedNode) || cachedNode.config in externalNodeConfigs) {
                logger.info("searching for a new node ...")
                for (config in (localNodeConfigs + externalNodeConfigs)) {
                    val node = NodeReference(config, connect(config))
                    if (isNodeHealthy(node)) {
                        logger.info("found available node $config")
                        currentNode.set(node)
                        return@withLock node
                    }
                }
            }
            return@withLock cachedNode
        }
    }

    private suspend fun isNodeHealthy(node: NodeReference<CONFIG, NODE>): Boolean {
        return try {
            isHealthy(node.config, node.handle)
        } catch (e: Exception) {
            logger.warn("error while checking node health for ${node.config}", e)
            false
        }
    }

    private inner class MonitoringThread : Thread() {
        @Volatile
        private var isRunning = true

        override fun run() {
            while (isRunning) {
                try {
                    runBlocking { findAvailableNode() }
                } catch (e: Exception) {
                    logger.error("error while monitoring nodes", e)
                }
                sleep(monitoringInterval.toMillis())
            }
        }

        fun stopThread() {
            isRunning = false
            interrupt()
        }
    }

    override fun close() {
        monitoringThread.stopThread()
    }
}
