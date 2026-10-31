package com.example.kafkademo;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WorkflowTest {
    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, OrderEvent> template() { return mock(KafkaTemplate.class); }

    @Test void httpResultWaitsForBrokerAcknowledgement() {
        var template = template();
        var pending = new CompletableFuture<SendResult<String, OrderEvent>>();
        when(template.send(eq("orders"), anyString(), any(OrderEvent.class))).thenReturn(pending);
        var response = new OrderController(template).createOrder(new CreateOrderRequest("c1", BigDecimal.TEN));
        assertFalse(response.isDone());
        pending.complete(null);
        assertEquals("c1", response.join().customerId());
    }
    @Test void brokerFailureDoesNotBecomeSuccessfulAcceptance() {
        var template = template();
        when(template.send(eq("orders"), anyString(), any(OrderEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        var response = new OrderController(template).createOrder(new CreateOrderRequest("c1", BigDecimal.TEN));
        assertTrue(response.isCompletedExceptionally());
    }
    @Test void invalidAmountNeverPublishesShippingInput() {
        var template = template();
        var journal = new DemoJournal();
        var listener = new InventoryListener(journal, template);
        assertThrows(IllegalStateException.class,
                () -> listener.reserveStock(new OrderEvent("bad", "c1", BigDecimal.ZERO)));
        verifyNoInteractions(template);
        assertEquals("inventory-attempt", journal.entries().get(0).stage());
    }
    @Test void downstreamPublishFailureIsPropagatedForRetry() {
        var template = template();
        var event = new OrderEvent("good", "c1", BigDecimal.TEN);
        when(template.send("inventory.reserved", "good", event))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        var journal = new DemoJournal();
        assertThrows(Exception.class, () -> new InventoryListener(journal, template).reserveStock(event));
        assertTrue(journal.entries().stream().noneMatch(e -> e.stage().equals("inventory-success")));
    }
}
