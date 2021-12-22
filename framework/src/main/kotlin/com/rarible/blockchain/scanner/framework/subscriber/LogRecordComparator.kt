package com.rarible.blockchain.scanner.framework.subscriber

import com.rarible.blockchain.scanner.framework.model.LogRecord

interface LogRecordComparator<LR : LogRecord> : Comparator<LR>
