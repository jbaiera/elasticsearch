/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Tuple;

/**
 * A binding of a bulk metadata service to a specific bulk metadata operation along with any applicable state needed for its execution.
 * By executing all operations internally in this binding, they can maintain type safety. This object is immutable to maintain thread
 * safety when state must be updated with results from asynchronous operations. Methods for phases that can mutate the state of the
 * operation will return a new instance.
 * <p>
 * The order in which these task methods are executed would be reminiscent of the following:
 * <code><pre>
 * BulkMetadataServiceTask&lt;?,?,?&gt; task = bulkMetadataService.createTask(operations);
 * task.validateBatch(currentMetadata);
 * task = task.filterBatch(currentMetadata);
 * task.collectDependencies(ActionListener.wrap((task, e) -> {
 *     // Error handling
 *     task.dependencyValidation(currentMetadata);
 *     // ... Submit Cluster State Update
 *         Tuple&lt;ProjectMetadata, BulkMetadataServiceTask&lt;?,?,?&gt;&gt; tuple = task.applyBatch(currentMetadata);
 *         tuple.v2().validateFinalClusterState(currentMetadata, tuple.v1());
 *         return tuple.v1();
 *     // --- End Cluster State Update
 * });
 * </pre></code>
 * This allows the tasks to be pipelined together with tasks from other services to update project state in one go.
 * </p>
 */
public class BulkMetadataServiceTask<B extends BulkMetadataService.BulkMetadataOperation, D, C> {
    private final BulkMetadataService<B, D, C> service;
    private final B operations;
    private final D dependency;
    private final C context;
    private final boolean skippable;

    public BulkMetadataServiceTask(BulkMetadataService<B, D, C> service, B operations) {
        this.service = service;
        this.operations = operations;
        this.dependency = null;
        this.context = null;
        this.skippable = operations.isEmpty();
    }

    private BulkMetadataServiceTask(BulkMetadataService<B, D, C> service, B operations, D dependency, C context) {
        this.service = service;
        this.operations = operations;
        this.dependency = dependency;
        this.context = context;
        this.skippable = operations.isEmpty();
    }

    /**
     * @return true if the internal list of operations is empty or there is nothing applicable to run,
     * false if this task has work to be done.
     */
    boolean isSkippable() {
        return skippable;
    }

    /**
     * Validates the operations against the bound service. If this task is skippable, no validations are executed.
     * @param currentMetadata The current state of the project.
     * @throws Exception In the event of a validation error.
     */
    void validateBatch(ProjectMetadata currentMetadata) throws Exception {
        if (skippable == false) {
            service.validateBatch(operations, currentMetadata);
        }
    }

    /**
     * Filters the operations using the bound service. If this task is skippable, no filtering takes place.
     * @param currentMetadata The current state of the project.
     * @return A new instance of this state object containing the updated collection of operations, or the same instance if the
     * operation was empty.
     */
    BulkMetadataServiceTask<B, D, C> filterBatch(ProjectMetadata currentMetadata) {
        if (skippable) {
            return this;
        } else {
            B filteredOperations = service.filterBatch(operations, currentMetadata);
            return new BulkMetadataServiceTask<>(service, filteredOperations, dependency, context);
        }
    }

    /**
     * The bound service launches an asynchronous process to collect dependent information from the cluster if applicable.
     * If this task is skippable, dependency collection is skipped.
     * @param listener If dependencies are collected, the listener will be completed with a new instance of this state object
     *                 containing the collected dependencies, or the same instance if no dependencies were required or the operation
     *                 is empty.
     */
    void collectDependencies(ActionListener<BulkMetadataServiceTask<?, ?, ?>> listener) {
        if (skippable) {
            listener.onResponse(this);
        } else {
            service.collectDependencies(
                operations,
                listener.delegateFailureAndWrap(
                    (l, dep) -> l.onResponse(dep == null ? this : new BulkMetadataServiceTask<>(service, operations, dep, context))
                )
            );
        }
    }

    /**
     * Validates any collected dependencies using the bound service. If this task is skippable, no validation is performed.
     * @param currentMetadata The current state of the project after dependencies were collected.
     * @throws Exception In the event of a validation error.
     */
    void dependencyValidation(ProjectMetadata currentMetadata) throws Exception {
        if (skippable == false) {
            service.dependencyValidation(operations, currentMetadata, dependency);
        }
    }

    /**
     * Applies the operation to the metadata builder using the bound service. If the task is skippable, no changes are
     * made to the project builder.
     * @param currentMetadata The current state of the project within the cluster state update task.
     * @return If a context was returned from the apply operation, a new instance of this state object containing that context, or
     * the same instance if no context was returned or the operation is empty.
     * @throws Exception If encountering a problem with updating the project metadata.
     */
    Tuple<ProjectMetadata, BulkMetadataServiceTask<B, D, C>> applyBatch(ProjectMetadata currentMetadata) throws Exception {
        if (skippable) {
            return Tuple.tuple(currentMetadata, this);
        } else {
            BulkMetadataService.BulkMetadataOperationContext<C> ctx = service.applyBatch(operations, currentMetadata);
            if (ctx == null) {
                assert false
                    : "BulkMetadataService implementation ["
                    + service.getBulkMetadataServiceName()
                    + "] returned null context from applyBatch method.";
                return Tuple.tuple(currentMetadata, this);
            } else {
                var newOperation = ctx.getContext()
                    .map(c -> new BulkMetadataServiceTask<>(service, operations, dependency, c))
                    .orElse(this);
                return Tuple.tuple(ctx.getResult(), newOperation);
            }
        }
    }

    /**
     * Validates the final project metadata against the bound service. If this task is skippable, no validations are executed.
     * @param previousMetadata The current state of the project.
     * @param newMetadata The updated state of the project.
     * @throws Exception In the event of a validation error.
     */
    void validateFinalClusterState(ProjectMetadata previousMetadata, ProjectMetadata newMetadata) throws Exception {
        if (skippable == false) {
            service.validateFinalProjectMetadata(previousMetadata, newMetadata, context);
        }
    }
}
