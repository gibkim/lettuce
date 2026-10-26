/*
 * Copyright (c) 2026-Present, Redis Ltd. All rights reserved.
 * SPDX-License-Identifier: MIT
 */
package io.lettuce.core.masterreplica;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.lettuce.core.RedisURI;
import io.lettuce.core.event.jfr.EventRecorder;
import io.lettuce.core.models.role.RedisNodeDescription;
import io.lettuce.core.resource.ClientResources;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Background topology refresh for a Sentinel-managed Master/Replica connection.
 * <p>
 * Sentinel only triggers a refresh when it publishes a topology-related message. If a refresh comes back without any node,
 * typically because none of the nodes could be reached at that moment, the connection would otherwise stay without known nodes
 * until the next such message. This task therefore schedules another refresh, backing off with the client's
 * {@link ClientResources#reconnectDelay() reconnect delay}, until a refresh returns nodes or the connection is closed.
 */
class SentinelTopologyRefreshTask implements Runnable {

    private static final InternalLogger LOG = InternalLoggerFactory.getInstance(SentinelTopologyRefreshTask.class);

    private final RedisURI redisURI;

    private final MasterReplicaTopologyRefresh refresh;

    private final MasterReplicaConnectionProvider<?, ?> connectionProvider;

    private final ClientResources clientResources;

    private final AtomicLong emptyRefreshAttempts = new AtomicLong();

    private final AtomicReference<ScheduledFuture<?>> pendingRetry = new AtomicReference<>();

    private volatile boolean closed;

    SentinelTopologyRefreshTask(RedisURI redisURI, MasterReplicaTopologyRefresh refresh,
            MasterReplicaConnectionProvider<?, ?> connectionProvider, ClientResources clientResources) {
        this.redisURI = redisURI;
        this.refresh = refresh;
        this.connectionProvider = connectionProvider;
        this.clientResources = clientResources;
    }

    @Override
    public void run() {

        if (closed) {
            return;
        }

        try {

            LOG.debug("Refreshing topology");
            refresh.getNodesAsync(redisURI).thenAccept(this::onTopology).exceptionally(t -> {
                LOG.error("Error during background refresh", t);
                return null;
            });

        } catch (Exception e) {
            LOG.error("Error during background refresh", e);
        }
    }

    private void onTopology(List<RedisNodeDescription> nodes) {

        if (closed) {
            return;
        }

        EventRecorder.getInstance().record(new MasterReplicaTopologyChangedEvent(redisURI, nodes));

        LOG.debug("New topology: {}", nodes);
        connectionProvider.setKnownNodes(nodes);

        if (nodes.isEmpty()) {
            scheduleRetry();
        } else {
            emptyRefreshAttempts.set(0);
            cancelPendingRetry();
        }
    }

    private void scheduleRetry() {

        long attempt = emptyRefreshAttempts.incrementAndGet();
        Duration delay = clientResources.reconnectDelay().createDelay(attempt);

        LOG.warn("Topology refresh returned no nodes from {}, retrying in {} ms", redisURI, delay.toMillis());

        ScheduledFuture<?> retry = clientResources.eventExecutorGroup().schedule(() -> {
            pendingRetry.set(null);
            run();
        }, delay.toMillis(), TimeUnit.MILLISECONDS);

        ScheduledFuture<?> previous = pendingRetry.getAndSet(retry);
        if (previous != null) {
            previous.cancel(false);
        }

        if (closed) {
            cancelPendingRetry();
        }
    }

    private void cancelPendingRetry() {

        ScheduledFuture<?> retry = pendingRetry.getAndSet(null);
        if (retry != null) {
            retry.cancel(false);
        }
    }

    /**
     * Stop refreshing and cancel a pending retry.
     */
    void close() {
        closed = true;
        cancelPendingRetry();
    }

    boolean isClosed() {
        return closed;
    }

}
