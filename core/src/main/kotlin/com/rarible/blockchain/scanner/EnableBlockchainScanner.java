package com.rarible.blockchain.scanner;

import com.rarible.blockchain.scanner.configuration.BlockchainScannerConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(BlockchainScannerConfiguration.class)
public @interface EnableBlockchainScanner {

}
