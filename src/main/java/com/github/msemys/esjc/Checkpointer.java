package com.github.msemys.esjc;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface Checkpointer {
    CompletableFuture<Void> onCheckpoint(Subscription subscription, Position position);
}