/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.loadbalance.extensions;

import static org.apache.pulsar.broker.loadbalance.LoadManager.LOADBALANCE_BROKERS_ROOT;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.CreateOption;

/**
 * The broker registry impl, base on the LockManager.
 */
@Slf4j
public class BrokerRegistryImpl implements BrokerRegistry {

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final BrokerLookupData brokerLookupData;

    private final MetadataCache<BrokerLookupData> brokerLookupDataMetadataCache;

    private final String brokerIdKeyPath;

    private final ScheduledExecutorService scheduler;

    private final List<BiConsumer<String, NotificationType>> listeners;

    protected enum State {
        Init,
        Started,
        Registered,
        Closed
    }

    private State state;

    public BrokerRegistryImpl(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.conf = pulsar.getConfiguration();
        this.brokerLookupDataMetadataCache = pulsar.getLocalMetadataStore().getMetadataCache(BrokerLookupData.class);
        this.scheduler = pulsar.getLoadManagerExecutor();
        this.listeners = new ArrayList<>();
        this.brokerIdKeyPath = keyPath(pulsar.getBrokerId());
        this.brokerLookupData = new BrokerLookupData(
                pulsar.getWebServiceAddress(),
                pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(),
                pulsar.getBrokerServiceUrlTls(),
                pulsar.getAdvertisedListeners(),
                pulsar.getProtocolDataToAdvertise(),
                pulsar.getConfiguration().isEnablePersistentTopics(),
                pulsar.getConfiguration().isEnableNonPersistentTopics(),
                conf.getLoadManagerClassName(),
                System.currentTimeMillis(),
                pulsar.getBrokerVersion(),
                pulsar.getConfig().lookupProperties());
        this.state = State.Init;
    }

    @Override
    public synchronized void start() throws PulsarServerException {
        if (this.state != State.Init) {
            return;
        }
        pulsar.getLocalMetadataStore().registerListener(this::handleMetadataStoreNotification);
        try {
            this.state = State.Started;
            this.register();
        } catch (MetadataStoreException e) {
            throw new PulsarServerException(e);
        }
    }

    @Override
    public boolean isStarted() {
        return this.state == State.Started || this.state == State.Registered;
    }

    @Override
    public synchronized void register() throws MetadataStoreException {
        if (this.state == State.Started) {
            try {
                brokerLookupDataMetadataCache.put(brokerIdKeyPath, brokerLookupData, EnumSet.of(CreateOption.Ephemeral))
                        .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
                this.state = State.Registered;
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw MetadataStoreException.unwrap(e);
            }
        }
    }

    @Override
    public synchronized void unregister() throws MetadataStoreException {
        if (this.state == State.Registered) {
            try {
                brokerLookupDataMetadataCache.delete(brokerIdKeyPath)
                        .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof MetadataStoreException.NotFoundException) {
                    log.warn("{} has already been unregistered", brokerIdKeyPath);
                } else {
                    throw MetadataStoreException.unwrap(e);
                }
            } catch (InterruptedException | TimeoutException e) {
                throw MetadataStoreException.unwrap(e);
            } finally {
                this.state = State.Started;
            }
        }
    }

    @Override
    public String getBrokerId() {
        return pulsar.getBrokerId();
    }

    @Override
    public CompletableFuture<List<String>> getAvailableBrokersAsync() {
        this.checkState();
        return brokerLookupDataMetadataCache.getChildren(LOADBALANCE_BROKERS_ROOT);
    }

    @Override
    public CompletableFuture<Optional<BrokerLookupData>> lookupAsync(String broker) {
        this.checkState();
        return brokerLookupDataMetadataCache.get(keyPath(broker));
    }

    public CompletableFuture<Map<String, BrokerLookupData>> getAvailableBrokerLookupDataAsync() {
        this.checkState();
        return this.getAvailableBrokersAsync().thenCompose(availableBrokers -> {
            Map<String, BrokerLookupData> map = new ConcurrentHashMap<>();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String brokerId : availableBrokers) {
                futures.add(this.lookupAsync(brokerId).thenAccept(lookupDataOpt -> {
                    if (lookupDataOpt.isPresent()) {
                        map.put(brokerId, lookupDataOpt.get());
                    } else {
                        log.warn("Got an empty lookup data, brokerId: {}", brokerId);
                    }
                }));
            }
            return FutureUtil.waitForAll(futures).thenApply(__ -> map);
        });
    }

    public synchronized void addListener(BiConsumer<String, NotificationType> listener) {
        this.checkState();
        this.listeners.add(listener);
    }

    @Override
    public synchronized void close() throws PulsarServerException {
        if (this.state == State.Closed) {
            return;
        }
        try {
            this.listeners.clear();
            this.unregister();
        } catch (Exception ex) {
            log.error("Unexpected error when unregistering the broker registry", ex);
        } finally {
            this.state = State.Closed;
        }
    }

    private void handleMetadataStoreNotification(Notification t) {
        if (!this.isStarted() || !isVerifiedNotification(t)) {
            return;
        }
        try {
            if (log.isDebugEnabled()) {
                log.debug("Handle notification: [{}]", t);
            }
            if (listeners.isEmpty()) {
                return;
            }
            this.scheduler.submit(() -> {
                String brokerId = t.getPath().substring(LOADBALANCE_BROKERS_ROOT.length() + 1);
                for (BiConsumer<String, NotificationType> listener : listeners) {
                    listener.accept(brokerId, t.getType());
                }
            });
        } catch (RejectedExecutionException e) {
            // Executor is shutting down
        }
    }

    @VisibleForTesting
    protected static boolean isVerifiedNotification(Notification t) {
       return t.getPath().startsWith(LOADBALANCE_BROKERS_ROOT + "/")
               && t.getPath().length() > LOADBALANCE_BROKERS_ROOT.length() + 1;
    }

    @VisibleForTesting
    protected static String keyPath(String brokerId) {
        return String.format("%s/%s", LOADBALANCE_BROKERS_ROOT, brokerId);
    }

    private void checkState() throws IllegalStateException {
        if (this.state == State.Closed) {
            throw new IllegalStateException("The registry already closed.");
        }
    }
}
