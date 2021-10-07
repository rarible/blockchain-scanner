package com.rarible.blockchain.scanner.flow.subscriber

import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber

interface FlowLogEventSubscriber :
    LogEventSubscriber<FlowBlockchainBlock, FlowBlockchainLog, FlowLog, FlowLogRecord<*>, FlowDescriptor>
