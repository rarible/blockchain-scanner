package com.rarible.blockchain.scanner.framework.subscriber

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

interface LogEventComparator<L : Log<L>, LR : LogRecord<L, *>> : Comparator<LR>