package com.rarible.blockchain.scanner.framework.subscriber

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

interface LogEventComparator<L : Log, LR : LogRecord<L>> : Comparator<LR>
