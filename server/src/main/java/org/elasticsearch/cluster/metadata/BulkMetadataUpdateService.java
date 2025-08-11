/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.BulkMetadataService.BulkMetadataOperation;

/**
 * Combines multiple project metadata changes from various metadata services into a single cluster state update which is applied and
 * validated atomically. Services that implement {@link BulkMetadataService} are registered with this update service. This service is
 * able to bulk-apply any metadata changes for which it has a corresponding {@link BulkMetadataService} implementation.
 */
public class BulkMetadataUpdateService {

    private static final Logger logger = LogManager.getLogger(BulkMetadataUpdateService.class);

    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<BulkMetadataClusterStateUpdateTask> taskQueue;
    private final ProjectResolver projectResolver;

    /**
     * This is the cluster state task executor for all bulk content installation actions.
     */
    private static final SimpleBatchedExecutor<BulkMetadataClusterStateUpdateTask, Void> BULK_METADATA_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(BulkMetadataClusterStateUpdateTask task, ClusterState clusterState)
                throws Exception {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(BulkMetadataClusterStateUpdateTask task, Void unused) {
                task.listener.onResponse(AcknowledgedResponse.TRUE);
            }
        };

    private abstract static class BulkMetadataClusterStateUpdateTask implements ClusterStateTaskListener {
        final ActionListener<AcknowledgedResponse> listener;

        BulkMetadataClusterStateUpdateTask(
            ActionListener<AcknowledgedResponse> listener
        ) {
            this.listener = listener;
        }

        public abstract ClusterState execute(ClusterState currentState) throws Exception;

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    // PRTODO: This needs to be constructed at node creation time from plugins
    final Map<String, BulkMetadataService<?,?,?>> serviceRegistry = new HashMap<>();

    @Inject
    public BulkMetadataUpdateService(
        ClusterService clusterService,
        MetadataIndexTemplateService metadataIndexTemplateService,
        IngestService ingestService,
        ProjectResolver projectResolver
    ) {
        this.clusterService = clusterService;
        this.taskQueue = clusterService.createTaskQueue("plugin-contents", Priority.URGENT, BULK_METADATA_TASK_EXECUTOR);
        this.projectResolver = projectResolver;

        // PRTODO: These need to be discovered via plugin or something
        serviceRegistry.put(
            metadataIndexTemplateService.getBulkMetadataServiceName(),
            metadataIndexTemplateService
        );
        serviceRegistry.put(
            ingestService.getBulkMetadataServiceName(),
            ingestService
        );
    }

    /**
     * The collection of metadata operations that will be executed in bulk
     */
    public static final class Request {
        private final Map<String, BulkMetadataOperation> operations;
        private final TimeValue masterTimeout;

        private Request(
            Map<String, BulkMetadataOperation> operations,
            TimeValue masterTimeout
        ) {
            this.operations = operations;
            this.masterTimeout = masterTimeout;
        }

        public Collection<BulkMetadataOperation> operations() {
            return operations.values();
        }

        public TimeValue masterTimeout() {
            return masterTimeout;
        }

        public static Builder builder(TimeValue masterTimeout) {
            return new Builder(masterTimeout);
        }

        public static class Builder {
            private final Map<String, BulkMetadataOperation> operations = new HashMap<>();
            private TimeValue masterTimeout;

            private Builder(TimeValue masterTimeout) {
                this.masterTimeout = Objects.requireNonNull(masterTimeout, "masterTimeout");
            }

            public Builder addToRequest(BulkMetadataOperation operation) {
                BulkMetadataOperation existingOperation = operations.get(operation.getBulkMetadataServiceName());
                if (existingOperation != null) {
                    throw new IllegalArgumentException(
                        "Trying to add operation of type ["
                            + operation.getBulkMetadataServiceName()
                            + "] but one is already present in this package"
                    );
                }
                operations.put(operation.getBulkMetadataServiceName(), operation);
                return this;
            }

            public Builder masterTimeout(TimeValue masterTimeout) {
                this.masterTimeout = Objects.requireNonNull(masterTimeout, "masterTimeout");
                return this;
            }

            public Request build() {
                return new Request(Collections.unmodifiableMap(operations), masterTimeout);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (Request) obj;
            return Objects.equals(this.operations, that.operations) &&
                Objects.equals(this.masterTimeout, that.masterTimeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operations, masterTimeout);
        }

        @Override
        public String toString() {
            return "Request[" +
                "operations=" + operations + ", " +
                "masterTimeout=" + masterTimeout + ']';
        }
    }

    public void bulkUpdateMetadata(final String cause, final Request request, final ActionListener<AcknowledgedResponse> listener) {
        // Wrap each of the bulk operations with their corresponding services that handle them
        final List<BulkMetadataServiceTask<?, ?, ?>> initialServiceTasks = new ArrayList<>(request.operations.entrySet().size());
        for (BulkMetadataOperation operation : request.operations.values()) {
            BulkMetadataService<?, ?, ?> updateService = serviceRegistry.get(operation.getBulkMetadataServiceName());
            if (updateService == null) {
                throw new IllegalArgumentException(
                    "Invalid package type [" + operation.getBulkMetadataServiceName() + "] given, no factory registered"
                );
            }
            initialServiceTasks.add(updateService.createTask(operation));
        }

        // Obtain the project id for this bulk request
        final ProjectId projectId = projectResolver.getProjectId();

        SubscribableListener.newForked((ActionListener<Collection<BulkMetadataServiceTask<?, ?, ?>>> l) -> {
            final ProjectMetadata projectMetadata = clusterService.state().getMetadata().getProject(projectId);

            forEachTask(initialServiceTasks, (service) -> service.validateBatch(projectMetadata));
            final var filteredOperations = mapTask(initialServiceTasks, (service) -> service.filterBatch(projectMetadata))
                .stream()
                .filter(Predicate.not(BulkMetadataServiceTask::isSkippable))
                .toList();

            if (filteredOperations.isEmpty()) {
                // Short circuit past dependency collection if there are no operations to perform
                l.onResponse(filteredOperations);
                return;
            }

            final var dependencyListener = new GroupedActionListener<>(filteredOperations.size(), l);
            forEachTask(filteredOperations, (service) -> service.collectDependencies(dependencyListener));
        }).andThen((final ActionListener<AcknowledgedResponse> l, final Collection<BulkMetadataServiceTask<?, ?, ?>> serviceOperations) -> {
            if (serviceOperations.isEmpty()) {
                // Short circuit any further processing if there are no operations to perform
                l.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            final ProjectMetadata projectMetadata = clusterService.state().getMetadata().getProject(projectId);
            forEachTask(serviceOperations, (service) -> service.dependencyValidation(projectMetadata));

            taskQueue.submitTask(
                "bulk-metadata-update, cause [" + cause + "]",
                new BulkMetadataClusterStateUpdateTask(l) {
                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        final ProjectMetadata initialProject = clusterService.state().getMetadata().getProject(projectId);
                        ProjectMetadata projectMetadata = initialProject;

                        final var appliedOperations = new ArrayList<BulkMetadataServiceTask<?, ?, ?>>(serviceOperations.size());
                        for (BulkMetadataServiceTask<?, ?, ?> serviceOperation : serviceOperations) {
                            var tuple = serviceOperation.applyBatch(projectMetadata);
                            projectMetadata = tuple.v1();
                            appliedOperations.add(tuple.v2());
                        }

                        if (projectMetadata == initialProject) {
                            return currentState;
                        }

                        final ProjectMetadata updatedProject = projectMetadata;
                        forEachTask(appliedOperations, (service) -> service.validateFinalClusterState(initialProject, updatedProject));
                        return ClusterState.builder(currentState).putProjectMetadata(projectMetadata).build();
                    }
                },
                request.masterTimeout
            );
        }).addListener(listener);
    }

    /**
     * Executes the given function on each task, collecting the resulting task into a new list to be returned. This is more
     * exception friendly than the normal {@link java.util.stream.Stream#map(Function)}.
     * @param tasks The tasks to execute over
     * @param function The function to execute on each task
     * @return a new collection that contains the updated tasks
     * @throws Exception Any exception that is thrown from a function
     */
    private static Collection<BulkMetadataServiceTask<?, ?, ?>> mapTask(
        Collection<BulkMetadataServiceTask<?, ?, ?>> tasks,
        CheckedFunction<BulkMetadataServiceTask<?, ?, ?>, BulkMetadataServiceTask<?, ?, ?>, Exception> function
    ) throws Exception {
        var result = new ArrayList<>(tasks);
        for (BulkMetadataServiceTask<?, ?, ?> serviceOperation : tasks) {
            result.add(function.apply(serviceOperation));
        }
        return result;
    }

    /**
     * Executes the given consumer on each task. This is more exception friendly than the normal {@link Collection#forEach(Consumer)}.
     * @param tasks The tasks to execute over
     * @param consumer The consumer to execute on each task
     * @throws Exception Any exception that is thrown from a consumer
     */
    private static void forEachTask(
        Collection<BulkMetadataServiceTask<?, ?, ?>> tasks,
        CheckedConsumer<BulkMetadataServiceTask<?, ?, ?>, Exception> consumer
    ) throws Exception {
        for (BulkMetadataServiceTask<?, ?, ?> serviceOperation : tasks) {
            consumer.accept(serviceOperation);
        }
    }
}
