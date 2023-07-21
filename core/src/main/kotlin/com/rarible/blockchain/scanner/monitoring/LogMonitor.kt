package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.model.Descriptor
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import java.util.concurrent.ConcurrentHashMap

class LogMonitor(
    properties: BlockchainScannerProperties,
    meterRegistry: MeterRegistry
) : AbstractMonitor(
    properties,
    meterRegistry,
    "log"
) {
    private var onPrepareLogs: Timer? = null

    private var onSaveLogs: Timer? = null

    private var onPublishLogs: Timer? = null

    private val logCounters = ConcurrentHashMap<String, Counter>()

    override fun register() {
        onPrepareLogs = addTimer("on_prepare_logs")
        onSaveLogs = addTimer("on_save_logs")
        onPublishLogs = addTimer("on_publish_logs")
    }

    override fun refresh() = Unit

    private fun getInsertedLogsCounter(descriptor: Descriptor): Counter =
        logCounters.getOrPut(descriptor.id) {
            addCounter(
                metricName = "inserted_logs",
                Tag.of("subscriber", descriptor.alias ?: descriptor.id)
            )
        }

    fun onLogsInserted(descriptor: Descriptor, inserted: Int) {
        getInsertedLogsCounter(descriptor).increment(inserted.toDouble())
    }

    suspend fun <T> onPrepareLogs(block: suspend () -> T): T {
        return recordTime(onPrepareLogs, block)
    }

    suspend fun <T> onSaveLogs(block: suspend () -> T): T {
        return recordTime(onSaveLogs, block)
    }

    suspend fun <T> onPublishLogs(block: suspend () -> T): T {
        return recordTime(onPublishLogs, block)
    }
}
