package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.test.repository.TestLogStorage

data class TestDescriptor(
    val topic: String,
    val contracts: List<String>,
    override val groupId: String = topic,
    override val id: String = topic,
    val saveLogs: Boolean = true,
    override val storage: TestLogStorage,
) : Descriptor<TestLogStorage> {
    override fun shouldSaveLogs() = saveLogs
}
