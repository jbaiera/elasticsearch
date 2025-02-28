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

import java.util.Optional;

import static org.elasticsearch.cluster.metadata.BulkMetadataService.BulkMetadataOperation;

/**
 * An interface that defines the process for bulk modifications to the cluster state and metadata. Most cluster state changes are batched
 * together in some form, but each of the batched operations are applied individually to the cluster state in a serial manner. Services
 * that implement this interface can group multiple operations together to be applied to a cluster state change atomically, as a single
 * operation in a batched cluster state update.
 * <p>
 *     When submitting a bulk metadata operation to the cluster, every operation is executed in the following phases:
 *     <ol>
 *         <li>The operations are bound to the service that handles them in a new process object</li>
 *         <li>The batch of operations are checked for validation errors.</li>
 *         <li>The batch is transformed, with no-op operations removed, and any other dependent operations added if applicable.</li>
 *         <li>If the service needs additional information to validate the operation, it can be collected asynchronously.</li>
 *         <li>If dependencies were needed, they are validated after collection is complete.</li>
 *         <li>A cluster state operation is submitted and the operations are applied to the cluster state.</li>
 *         <li>After all operations have been applied, a final validation is run on the resulting cluster state to account for any
 *         changes made by other services.</li>
 *         <li>The cluster state change is concluded.</li>
 *     </ol>
 * </p>
 *
 * @param <B> The type of operation this service handles, instances of which contain the batch of operations that must be bulk applied.
 * @param <D> Type of dependency object. Instances may need to asynchronously collect dependencies before trying to apply a batch of
 *           operations. Those dependencies must be collected and stored in this type if needed.
 * @param <C> Type of context object to pass between phases. The service applies all changes to the cluster state or metadata in one phase
 *           and validates the changes in a separate phase. This allows multiple services to execute in the same cluster state operation.
 *           Intermediate state that must be shared between these phases is captured in an instance of this context type.
 */
public interface BulkMetadataService<B extends BulkMetadataOperation, D, C> {

    /**
     * Base class for a {@link BulkMetadataService}'s operation. Subclasses
     * should store the batch of changes for each operation within.
     */
    abstract class BulkMetadataOperation {
        protected String serviceKey;

        /**
         * @param serviceKey the unique name for the {@link BulkMetadataService} this operation corresponds to.
         * @see BulkMetadataService#getBulkMetadataServiceName()
         */
        public BulkMetadataOperation(String serviceKey) {
            this.serviceKey = serviceKey;
        }

        /**
         * @return the unique name for the {@link BulkMetadataService} this operation corresponds to.
         * @see BulkMetadataService#getBulkMetadataServiceName()
         */
        public String getBulkMetadataServiceName() {
            return serviceKey;
        }

        /**
         * @return true if this operation has no operations contained within it.
         */
        public abstract boolean isEmpty();
    }

    /**
     * A context object returned when applying a batch which contains result information as well as an optional context object which
     * may be used to pass information between the batch application phase and the final validation phase.
     * <p>
     *     Instances should be created using one of the available static factory methods:
     *     <pre><code>
     *         return BulkMetadataOperationContext.withContext(projectBuilder.build(), new MyContextObject(...));
     *         return BulkMetadataOperationContext.noContext(currentProject);
     *     </code></pre>
     * </p>
     * @param <T> The class of the optional context object which should be shared between the final two phases.
     * @see BulkMetadataService#applyBatch(BulkMetadataOperation, ProjectMetadata)
     * @see BulkMetadataService#validateFinalProjectMetadata(ProjectMetadata, ProjectMetadata, Object)
     */
    class BulkMetadataOperationContext<T> {

        /**
         * Indicates that state was modified by the metadata service. No context is stored within.
         * @param <T> The type of context object expected by subsequent calls to the service
         * @param projectMetadata The project metadata that resulted from the apply operation. If no changes were made, return the
         *                         original metadata object.
         * @return New response object
         */
        public static <T> BulkMetadataOperationContext<T> noContext(ProjectMetadata projectMetadata) {
            return new BulkMetadataOperationContext<>(projectMetadata, null);
        }

        /**
         * Indicates that state was modified by the metadata service. No context is stored within.
         * @param <T> The type of context object expected by subsequent calls to the service
         * @param projectMetadata The project metadata that resulted from the apply operation. If no changes were made, return the
         *                        original metadata object.
         * @param context The optional context object which will be passed to the final validation method in the service
         * @return New response object
         */
        public static <T> BulkMetadataOperationContext<T> withContext(ProjectMetadata projectMetadata, T context) {
            return new BulkMetadataOperationContext<>(projectMetadata, context);
        }

        private final ProjectMetadata result;
        private final T context;

        private BulkMetadataOperationContext(ProjectMetadata result, T context) {
            this.result = result;
            this.context = context;
        }

        /**
         * @return The resulting project metadata.
         */
        public ProjectMetadata getResult() {
            return result;
        }

        /**
         * @return an optional context object that is expected on subsequent calls to the metadata service.
         * @see BulkMetadataService#validateFinalProjectMetadata(ProjectMetadata, ProjectMetadata, Object)
         */
        public Optional<T> getContext() {
            return Optional.ofNullable(context);
        }
    }

    /**
     * @return a unique name among implementors that identifies this service.
     */
    String getBulkMetadataServiceName();

    /**
     * Binds a bulk metadata operation together with this service into a self-contained task that can pipeline together with other bulk
     * metadata services to update many different aspects of the project metadata in one cluster state update task.
     * @param operation The bulk update operation to bind into a task object
     * @return A task, that contains all the state needed to execute the bulk update over multiple phases
     */
    BulkMetadataServiceTask<B, D, C> createTask(BulkMetadataOperation operation);

    /**
     * Executes to determine if this batch of operations is valid before doing any further work. Identifies any immediate validation issues
     * before submitting a cluster state update task.
     * @param batch operations to perform in bulk
     * @param currentProject current state of the project
     */
    default void validateBatch(B batch, ProjectMetadata currentProject) throws Exception {}

    /**
     * Removes or adds entries to the operation list before doing any further work. Commonly used to remove entries from the batch that
     * would be no-ops on the current cluster state.
     * @param batch operations to perform in bulk
     * @param currentProject current state of the project
     * @return the filtered list of operations to perform
     */
    default B filterBatch(B batch, ProjectMetadata currentProject) {
        return batch;
    }

    /**
     * Collects and constructs dependencies for this bulk operation. Some operations may need to collect information from other nodes
     * before applying the change to the cluster state. This is executed before the state change is submitted.
     * @param listener to collect the dependency through
     */
    default void collectDependencies(B batch, ActionListener<D> listener) {
        listener.onResponse(null);
    }

    /**
     * Validates the batch of operations against any dependencies collected by this service. Commonly used to check if the operations are
     * supported by the cluster.
     * @param batch operations to perform in bulk
     * @param currentProject current state of the project
     * @param dependency object collected by the service previously to validate against
     */
    default void dependencyValidation(B batch, ProjectMetadata currentProject, D dependency) throws Exception {}

    /**
     * Applies the batch of operations to the metadata builder, optionally returning a context object to use when validating the final
     * cluster state.
     * @param batch operations to perform in bulk
     * @param currentProject current state of the project
     * @return a context object that is used to carry forward state in order to validate the final cluster state
     */
    BulkMetadataOperationContext<C> applyBatch(B batch, ProjectMetadata currentProject) throws Exception;

    /**
     * Uses the context object to validate the new cluster state before returning it.
     * @param previousProject previous project metadata
     * @param newProject candidate project metadata
     * @param context context for the applied cluster state operation
     */
    default void validateFinalProjectMetadata(ProjectMetadata previousProject, ProjectMetadata newProject, C context) throws Exception {}
}
