package com.example.secure;

import java.nio.file.Path;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.*;

/** Two real TLS client certificates let the broker decide who may publish. */
@Configuration
public class IdentityProducers {
    private final KafkaProperties settings;
    @Value("${demo.cert-dir}") private String certificates;
    // Spring supplies the helpers this object needs; we keep them for later method calls.
    public IdentityProducers(KafkaProperties settings) {
        this.settings=settings;
    }
    // Build a producer using one client certificate so Kafka can identify the writer.
    private DefaultKafkaProducerFactory<String,String> factory(String user) {
        var config=settings.buildProducerProperties();
        // Replace only the identity; keep TLS trust and the other safe producer settings.
        config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,Path.of(certificates,user+".p12").toAbsolutePath().toString());
        return new DefaultKafkaProducerFactory<>(config);
    }
    // Factory beans are closed by Spring so network threads do not leak on shutdown.
    // Create Alice's producer resources with her certificate. Spring closes them at shutdown.
    @Bean DefaultKafkaProducerFactory<String,String> aliceFactory() {
        return factory("alice");
    }
    // Create Bob's producer resources with his different certificate.
    @Bean DefaultKafkaProducerFactory<String,String> bobFactory() {
        return factory("bob");
    }
    // Give the app a publishing helper that uses Alice's producer identity.
    @Bean KafkaTemplate<String,String> aliceTemplate(@org.springframework.beans.factory.annotation.Qualifier("aliceFactory") DefaultKafkaProducerFactory<String,String> factory) {
        return new KafkaTemplate<>(factory);
    }
    // Give the app a publishing helper that uses Bob's producer identity.
    @Bean KafkaTemplate<String,String> bobTemplate(@org.springframework.beans.factory.annotation.Qualifier("bobFactory") DefaultKafkaProducerFactory<String,String> factory) {
        return new KafkaTemplate<>(factory);
    }
}
