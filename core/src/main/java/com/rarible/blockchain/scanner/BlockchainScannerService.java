package com.rarible.blockchain.scanner;

import com.rarible.blockchain.scanner.data.BlockEvent;
import com.rarible.blockchain.scanner.data.NewBlockEvent;
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock;
import com.rarible.blockchain.scanner.framework.client.BlockchainClient;
import com.rarible.blockchain.scanner.framework.model.Block;
import com.rarible.blockchain.scanner.framework.model.EventData;
import com.rarible.blockchain.scanner.framework.model.LogEvent;
import com.rarible.blockchain.scanner.framework.service.BlockService;
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor;
import com.rarible.core.logging.LoggerContext;
import com.rarible.core.logging.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;

import static java.util.Collections.emptyList;

@Component
public class BlockchainScannerService<OB extends BlockchainBlock, OL, B extends Block, L extends LogEvent, D extends EventData> {

    private static final Logger logger = LoggerFactory.getLogger(BlockchainScannerService.class);

    private final BlockService<B> blockService;
    private final BlockchainClient<OB, OL> blockchainClient;
    private final List<LogEventPostProcessor<L>> logEventPostProcessors;
    private final BlockScanner<OB, OL, B> blockScanner;
    private final BlockEventHandler<OB, OL, L, D> blockEventHandler;
    private final long maxProcessTime;

    public BlockchainScannerService(
            BlockEventHandler<OB, OL, L, D> blockEventHandler,
            BlockService<B> blockService,
            BlockchainClient<OB, OL> blockchainClient,
            List<LogEventPostProcessor<L>> logEventPostProcessors,
            BlockScanner<OB, OL, B> blockScanner,
            @Value("${ethereumMaxProcessTime:300000}") long maxProcessTime
    ) {
        this.blockEventHandler = blockEventHandler;

        this.maxProcessTime = maxProcessTime;

        this.blockService = blockService;
        this.blockchainClient = blockchainClient;
        this.logEventPostProcessors = logEventPostProcessors;
        this.blockScanner = blockScanner;
    }

    @PostConstruct
    public void init() {
        Mono.delay(Duration.ofMillis(1000))
                .thenMany(blockScanner.scan())
                .map(this::toNewBlockEvent)
                .timeout(Duration.ofMinutes(5))
                .concatMap(this::onBlock)
                .then(Mono.<Void>error(new IllegalStateException("disconnected")))
                .retryWhen(
                        Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(300))
                                .maxBackoff(Duration.ofMillis(2000))
                                .doAfterRetry(s -> logger.warn("retrying {}", s))
                )
                .subscribe(
                        n -> {
                        },
                        th -> logger.error("unable to process block events. should never happen", th)
                );
    }

    private NewBlockEvent toNewBlockEvent(BlockEvent<OB> event) {
        BlockchainBlock block = event.getBlock();
        return new NewBlockEvent(
                block.getNumber(),
                block.getHash(), // TODO ???
                block.getTimestamp(),
                event.getReverted() != null ? event.getReverted().getHash() : null // TODO There was Word type
        );
    }

    // TODO Should be in separate class
    public Mono<Void> reindexPendingBlock(B block) {
        return LoggingUtils.withMarker(marker -> {
            logger.info(marker, "reindexing block {}", block);
            return blockchainClient.getBlockMeta(block.getId()).flatMap(it -> {
                NewBlockEvent event = new NewBlockEvent(block.getId(), it.getHash(), it.getTimestamp(), null);
                return onBlock(event);
            });
        });
    }

    public Mono<Void> onBlock(NewBlockEvent event) {
        return LoggingUtils.withMarker(marker -> {
            logger.info(marker, "onBlockEvent {}", event);
            return onBlockEvent(event)
                    .collectList()
                    .flatMap(it -> postProcessLogs(it).thenReturn(Block.Status.SUCCESS))
                    .timeout(Duration.ofMillis(maxProcessTime))
                    .onErrorResume(ex -> {
                        logger.error(marker, "Unable to handle event " + event, ex);
                        return Mono.just(Block.Status.ERROR);
                    })
                    .flatMap(status -> blockService.updateBlockStatus(event.getNumber(), status))
                    .then()
                    .onErrorResume(ex -> {
                        logger.error(marker, "Unable to save block status " + event, ex);
                        return Mono.empty();
                    });
        })
                .subscriberContext(ctx -> LoggerContext.addToContext(ctx, event.getContextParams()));
    }

    private Flux<L> onBlockEvent(NewBlockEvent event) {
        return blockEventHandler.onBlockEvent(event);
    }

    private Mono<Void> postProcessLogs(List<L> logs) {
        return Flux.fromIterable(logEventPostProcessors != null ? logEventPostProcessors : emptyList())
                .flatMap(it -> it.postProcessLogs(logs))
                .then();
    }
}
