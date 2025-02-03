package com.rarible.blockchain.scanner.hedera.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainBlock
import com.rarible.blockchain.scanner.hedera.client.HederaBlockchainLog
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import com.rarible.blockchain.scanner.hedera.model.HederaLogRecord
import com.rarible.blockchain.scanner.hedera.model.HederaLogStorage

interface HederaLogEventSubscriber :
    LogEventSubscriber<HederaBlockchainBlock, HederaBlockchainLog, HederaLogRecord, HederaDescriptor, HederaLogStorage>
