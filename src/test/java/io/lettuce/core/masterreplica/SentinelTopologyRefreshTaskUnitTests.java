package io.lettuce.core.masterreplica;

import static io.lettuce.TestTags.UNIT_TEST;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.lettuce.core.RedisURI;
import io.lettuce.core.models.role.RedisInstance;
import io.lettuce.core.models.role.RedisNodeDescription;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.Delay;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Unit tests for {@link SentinelTopologyRefreshTask}.
 */
@Tag(UNIT_TEST)
@SuppressWarnings({ "unchecked", "rawtypes" })
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SentinelTopologyRefreshTaskUnitTests {

    private static final RedisURI SEED = RedisURI.Builder.sentinel("localhost", "mymaster").build();

    private static final List<RedisNodeDescription> NODES = Collections.singletonList(
            new RedisMasterReplicaNode("localhost", 6379, RedisURI.create("localhost", 6379), RedisInstance.Role.UPSTREAM));

    @Mock
    private MasterReplicaTopologyRefresh refresh;

    @Mock
    private MasterReplicaConnectionProvider<String, String> connectionProvider;

    @Mock
    private ClientResources clientResources;

    @Mock
    private EventExecutorGroup eventExecutors;

    @Mock
    private Delay delay;

    @Mock
    private ScheduledFuture scheduledFuture;

    @Captor
    private ArgumentCaptor<Runnable> retry;

    private SentinelTopologyRefreshTask sut;

    @BeforeEach
    void before() {

        when(clientResources.eventExecutorGroup()).thenReturn(eventExecutors);
        when(clientResources.reconnectDelay()).thenReturn(delay);
        when(delay.createDelay(anyLong())).thenReturn(Duration.ofMillis(100));
        when(eventExecutors.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(scheduledFuture);

        sut = new SentinelTopologyRefreshTask(SEED, refresh, connectionProvider, clientResources);
    }

    @Test
    void shouldApplyNonEmptyTopologyWithoutRetry() {

        when(refresh.getNodesAsync(SEED)).thenReturn(CompletableFuture.completedFuture(NODES));

        sut.run();

        verify(connectionProvider).setKnownNodes(NODES);
        verify(eventExecutors, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    void shouldScheduleRetryWhenTopologyIsEmpty() {

        when(refresh.getNodesAsync(SEED)).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()),
                CompletableFuture.completedFuture(NODES));

        sut.run();

        verify(eventExecutors).schedule(retry.capture(), eq(100L), eq(TimeUnit.MILLISECONDS));

        retry.getValue().run();

        verify(connectionProvider).setKnownNodes(NODES);
        verify(eventExecutors, times(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    void shouldBackOffWhileTopologyStaysEmpty() {

        when(refresh.getNodesAsync(SEED)).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        sut.run();
        verify(eventExecutors).schedule(retry.capture(), anyLong(), any(TimeUnit.class));
        retry.getValue().run();

        verify(delay).createDelay(1);
        verify(delay).createDelay(2);
    }

    @Test
    void shouldResetBackOffAfterNonEmptyTopology() {

        when(refresh.getNodesAsync(SEED)).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()),
                CompletableFuture.completedFuture(NODES), CompletableFuture.completedFuture(Collections.emptyList()));

        sut.run();
        verify(eventExecutors).schedule(retry.capture(), anyLong(), any(TimeUnit.class));
        retry.getValue().run();
        sut.run();

        verify(delay, times(2)).createDelay(1);
    }

    @Test
    void shouldNotScheduleRetryWhenRefreshFails() {

        CompletableFuture<List<RedisNodeDescription>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("boom"));
        when(refresh.getNodesAsync(SEED)).thenReturn(failed);

        sut.run();

        verify(connectionProvider, never()).setKnownNodes(any());
        verify(eventExecutors, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    void closeShouldCancelPendingRetryAndStopRefreshing() {

        when(refresh.getNodesAsync(SEED)).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        sut.run();
        verify(eventExecutors).schedule(retry.capture(), anyLong(), any(TimeUnit.class));

        sut.close();
        retry.getValue().run();

        verify(scheduledFuture).cancel(false);
        verify(refresh, times(1)).getNodesAsync(SEED);
        assertThat(sut.isClosed()).isTrue();
    }

}
