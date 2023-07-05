package org.acme;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MyReactiveMessagingApplication {

    // @Inject
    // @Channel("words-out")
    // Emitter<String> emitter;

    /**
     * Sends message to the "words-out" channel, can be used from a JAX-RS resource or any bean of your application.
     * Messages are sent to the broker.
     **/
    // void onStart(@Observes StartupEvent ev) {
        // Stream.of("Hello", "with", "SmallRye", "reactive", "message").forEach(string -> emitter.send(string));
    // }

    @Outgoing("words-out")
    Multi<String> produceWords() {
        return Multi.createFrom().<AtomicLong, String>generator(() -> new AtomicLong(), (i, e) -> {
            String item = "SmallRye" + i.incrementAndGet();
            // System.out.println(item);
            e.emit(item);
            return i;
        })
        .onItem().transformToUniAndConcatenate(s -> Uni.createFrom().item(s).onItem().delayIt().by(Duration.ofMillis(1)))
        ;
    }

    AtomicInteger c = new AtomicInteger(0);

    /**
     * Consume the message from the "words-in" channel, uppercase it and send it to the uppercase channel.
     * Messages come from the broker.
     * @throws InterruptedException
     **/
    @Incoming("words-in")
    @Outgoing("uppercase")
    // @Blocking(value = "toUpperCase", ordered = false)
    @RunOnVirtualThread
    public Message<String> toUpperCase(Message<String> message) throws InterruptedException {
        c.incrementAndGet();
        Thread.sleep(5000);
        // System.out.println(c.decrementAndGet() + " " + Vertx.currentContext() +  " " + Thread.currentThread());
        c.decrementAndGet();
        return message.withPayload(message.getPayload().toUpperCase());
    }

    /**
     * Consume the uppercase channel (in-memory) and print the messages.
     **/
    @Incoming("uppercase")
    public void sink(String word) {
        System.out.println(c.get() + " >> " + word + " " + Thread.currentThread());
    }
}
