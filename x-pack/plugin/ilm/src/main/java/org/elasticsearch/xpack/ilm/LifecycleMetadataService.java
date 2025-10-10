/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.BulkMetadataService;
import org.elasticsearch.cluster.metadata.BulkMetadataServiceTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCacheManagement;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.ilm.action.ReservedLifecycleAction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

public class LifecycleMetadataService implements BulkMetadataService<LifecycleMetadataService.BulkLifecycleOperation, Void, Void> {

    private static final Logger logger = LogManager.getLogger(LifecycleMetadataService.class);

    private static final String BULK_METADATA_SERVICE_NAME = "elasticsearch.xpack.ilm";

    private static final String PUT_SOURCE_PREFIX = "put-lifecycle-";
    private static final String DELETE_SOURCE_PREFIX = "delete-lifecycle-";
    private static final String BULK_SOURCE = "bulk-update-lifecycle";

    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final ThreadPool threadPool;
    private final ProjectResolver projectResolver;
    private final MasterServiceTaskQueue<BulkLifecyclePolicyTask> taskQueue;

    public LifecycleMetadataService(
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Client client,
        XPackLicenseState licenseState,
        ThreadPool threadPool,
        ProjectResolver projectResolver
    ) {
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.licenseState = licenseState;
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
        this.taskQueue = clusterService.createTaskQueue(
            "ilm-put-lifecycle-queue",
            Priority.NORMAL,
            new LifecycleMetadataService.IlmLifecycleExecutor()
        );
    }

    @Override
    public String getBulkMetadataServiceName() {
        return BULK_METADATA_SERVICE_NAME;
    }

    @Override
    public BulkMetadataServiceTask<BulkLifecycleOperation, Void, Void> createTask(
        BulkMetadataOperation op
    ) {
        return new BulkMetadataServiceTask<>(this, (BulkLifecycleOperation) op);
    }

    public static class BulkLifecycleOperation extends BulkMetadataOperation {
        final Collection<PutLifecycleRequest> requests;
        final Map<String, String> filteredHeaders;
        final Set<String> deletePolicies;
        final boolean verboseLogging;

        public BulkLifecycleOperation(
            Collection<PutLifecycleRequest> requests,
            Map<String, String> filteredHeaders,
            Set<String> deletePolicies,
            boolean verboseLogging
        ) {
            super(BULK_METADATA_SERVICE_NAME);
            this.requests = requests;
            this.filteredHeaders = filteredHeaders;
            this.deletePolicies = deletePolicies;
            this.verboseLogging = verboseLogging;
        }

        public static BulkLifecycleOperation singleUpdate(PutLifecycleRequest request, boolean verboseLogging) {
            return new BulkLifecycleOperation(List.of(request), null, Set.of(), verboseLogging);
        }

        public static BulkLifecycleOperation singleDelete(String request) {
            return new BulkLifecycleOperation(List.of(), null, Set.of(request), false);
        }

        @Override
        public boolean isEmpty() {
            return (requests == null || requests.isEmpty()) && (deletePolicies == null || deletePolicies.isEmpty());
        }
    }

    @Override
    public void validateBatch(BulkLifecycleOperation batch, ProjectMetadata currentProject) {
        for (PutLifecycleRequest request : batch.requests) {
            LifecyclePolicy.validatePolicyName(request.getPolicy().getName());
            request.getPolicy().maybeAddDeprecationWarningForFreezeAction(request.getPolicy().getName());
        }
    }

    @Override
    public BulkLifecycleOperation filterBatch(
        BulkLifecycleOperation batch,
        ProjectMetadata currentProject
    ) {
        // PRTODO: It might be nice to consolidate duplicate removals and remove updates that are destined to be removed anyway
        // We only need to capture headers and validate on put operations
        if (batch.requests.isEmpty() == false) {
            // headers from the thread context stored by the AuthenticationService to be shared between the
            // REST layer and the Transport layer here must be accessed within this thread and not in the
            // cluster state thread in the ClusterStateUpdateTask below since that thread does not share the
            // same context, and therefore does not have access to the appropriate security headers.
            Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(
                threadPool.getThreadContext(),
                clusterService.state()
            );

            List<PutLifecycleRequest> filteredRequests = new ArrayList<>(batch.requests);
            IndexLifecycleMetadata lifecycleMetadata = currentProject.custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
            for (PutLifecycleRequest request : batch.requests) {
                LifecyclePolicyMetadata existingPolicy = lifecycleMetadata.getPolicyMetadatas().get(request.getPolicy().getName());
                // Skip a request if it is a no-op (if the policy and filtered headers match exactly)
                if (isNoopUpdate(existingPolicy, request.getPolicy(), filteredHeaders) == false) {
                    filteredRequests.add(request);
                }
            }
            // Capture the filtered headers on the operation
            return new BulkLifecycleOperation(filteredRequests, filteredHeaders, batch.deletePolicies, batch.verboseLogging);
        } else {
            return batch;
        }
    }

    @Override
    public BulkMetadataOperationContext<Void> applyBatch(
        BulkLifecycleOperation batch,
        ProjectMetadata previousProject
    ) {
        return doApplyBatch(batch, previousProject, licenseState, xContentRegistry, client);
    }

    public static BulkMetadataOperationContext<Void> doApplyBatch(
        BulkLifecycleOperation batch,
        ProjectMetadata previousProject,
        XPackLicenseState licenseState,
        NamedXContentRegistry xContentRegistry,
        Client client
    ) {
        final IndexLifecycleMetadata currentMetadata = previousProject.custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);

        ProjectMetadata.Builder projectBuilder = null;
        SortedMap<String, LifecyclePolicyMetadata> newPolicies = null;
        boolean policyChanges = false;

        for (PutLifecycleRequest request : batch.requests) {
            Map<String, LifecyclePolicyMetadata> checkPolicies = newPolicies == null ? currentMetadata.getPolicyMetadatas() : newPolicies;
            final LifecyclePolicyMetadata existingPolicyMetadata = checkPolicies.get(request.getPolicy().getName());

            // Double-check for no-op in the state update task, in case it was changed/reset in the meantime
            if (isNoopUpdate(existingPolicyMetadata, request.getPolicy(), batch.filteredHeaders)) {
                continue;
            }

            validatePrerequisites(request.getPolicy(), previousProject, licenseState);
            long nextVersion = (existingPolicyMetadata == null) ? 1L : existingPolicyMetadata.getVersion() + 1L;
            LifecyclePolicyMetadata lifecyclePolicyMetadata = new LifecyclePolicyMetadata(
                request.getPolicy(),
                batch.filteredHeaders,
                nextVersion,
                Instant.now().toEpochMilli()
            );
            policyChanges = true;
            if (newPolicies == null) {
                newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());
            }
            LifecyclePolicyMetadata oldPolicy = newPolicies.put(lifecyclePolicyMetadata.getName(), lifecyclePolicyMetadata);
            if (batch.verboseLogging) {
                if (oldPolicy == null) {
                    logger.info("adding index lifecycle policy [{}]", request.getPolicy().getName());
                } else {
                    logger.info("updating index lifecycle policy [{}]", request.getPolicy().getName());
                }
            }

            if (oldPolicy != null) {
                if (projectBuilder == null) {
                    // Put off on making the project builder for as long as possible
                    projectBuilder = ProjectMetadata.builder(previousProject);
                }

                // This used to have logic for if the refresh failed, but it doesn't look like it gets caught and logged earlier on,
                // thus keeping this from ever having an issue.
                PhaseCacheManagement.updateIndicesForPolicy(
                    projectBuilder,
                    previousProject,
                    xContentRegistry,
                    client,
                    oldPolicy.getPolicy(),
                    lifecyclePolicyMetadata,
                    licenseState
                );
            }
        }

        if (batch.deletePolicies.isEmpty() == false) {
            // Most common case will likely be a single delete operation
            Map<String, List<String>> policiesRequiredByIndices = HashMap.newHashMap(batch.deletePolicies.size());
            for (IndexMetadata idxMeta : previousProject.indices().values()) {
                if (idxMeta.getLifecyclePolicyName() != null && batch.deletePolicies.contains(idxMeta.getLifecyclePolicyName())) {
                    String name = idxMeta.getIndex().getName();
                    policiesRequiredByIndices.computeIfAbsent(idxMeta.getLifecyclePolicyName(), k -> new ArrayList<>()).add(name);
                }
            }
            if (policiesRequiredByIndices.isEmpty() == false) {
                throw collectExceptionsForRequiredPolicies(policiesRequiredByIndices);
            }

            Map<String, LifecyclePolicyMetadata> checkPolicies = newPolicies == null ? currentMetadata.getPolicyMetadatas() : newPolicies;
            if (checkPolicies.keySet().containsAll(batch.deletePolicies) == false) {
                Set<String> missingPolicies = Sets.difference(batch.deletePolicies, checkPolicies.keySet());
                if (missingPolicies.size() == 1) {
                    throw new ResourceNotFoundException("Lifecycle policy not found: {}", missingPolicies.iterator().next());
                } else {
                    throw new ResourceNotFoundException("Lifecycle policies not found: {}", missingPolicies);
                }
            }
            if (newPolicies == null) {
                newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());
            }
            newPolicies.keySet().removeAll(batch.deletePolicies);
        }

        if (policyChanges) {
            IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentILMMode(previousProject));
            if (projectBuilder == null) {
                projectBuilder = ProjectMetadata.builder(previousProject);
            }
            projectBuilder.putCustom(IndexLifecycleMetadata.TYPE, newMetadata);
            return BulkMetadataOperationContext.noContext(projectBuilder.build());
        } else {
            return BulkMetadataOperationContext.noContext(previousProject);
        }
    }

    /**
     * Compiles together all policies that could not be deleted because they are still in use by indices into an
     * {@link IllegalArgumentException}, returning the first exception with all following exceptions suppressed.
     *
     * @param policiesRequiredByIndices A map of policy name to the list of indices that require it.
     * @return An {@link IllegalArgumentException} for the first policy in the map, with any further exceptions suppressed under it.
     */
    private static IllegalArgumentException collectExceptionsForRequiredPolicies(Map<String, List<String>> policiesRequiredByIndices) {
        IllegalArgumentException exception = null;
        for (Map.Entry<String, List<String>> entry : policiesRequiredByIndices.entrySet()) {
            String policyName = entry.getKey();
            List<String> indicesUsingPolicy = entry.getValue();
            IllegalArgumentException iae = new IllegalArgumentException(
                "Cannot delete policy ["
                    + policyName
                    + "]. It is in use by one or more indices: "
                    + indicesUsingPolicy
            );
            if (exception == null) {
                exception = iae;
            } else {
                exception.addSuppressed(iae);
            }
        }
        return exception;
    }

    public void addLifecycle(PutLifecycleRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        bulkLifecycleChange(
            BulkLifecycleOperation.singleUpdate(request, true),
            state,
            request.masterNodeTimeout(),
            request.ackTimeout(),
            listener
        );
    }

    public void deleteLifecycle(DeleteLifecycleAction.Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        bulkLifecycleChange(
            BulkLifecycleOperation.singleDelete(request.getPolicyName()),
            state,
            request.masterNodeTimeout(),
            request.ackTimeout(),
            listener
        );
    }

    /**
     * Returns 'true' if the ILM policy is effectually the same (same policy and headers), and thus can be a no-op update.
     */
    public static boolean isNoopUpdate(
        @Nullable LifecyclePolicyMetadata existingPolicy,
        LifecyclePolicy newPolicy,
        Map<String, String> filteredHeaders
    ) {
        if (existingPolicy == null) {
            return false;
        } else {
            return newPolicy.equals(existingPolicy.getPolicy()) && filteredHeaders.equals(existingPolicy.getHeaders());
        }
    }

    /**
     * Validate that the license level is compliant for searchable-snapshots, that any referenced snapshot
     * repositories exist, and that any referenced SLM policies exist.
     *
     * @param policy The lifecycle policy
     * @param projectMetadata The project metadata
     */
    private static void validatePrerequisites(LifecyclePolicy policy, ProjectMetadata projectMetadata, XPackLicenseState licenseState) {
        List<Phase> phasesWithSearchableSnapshotActions = policy.getPhases()
            .values()
            .stream()
            .filter(phase -> phase.getActions().containsKey(SearchableSnapshotAction.NAME))
            .toList();
        // check license level for searchable snapshots
        if (phasesWithSearchableSnapshotActions.isEmpty() == false
            && SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseState) == false) {
            throw new IllegalArgumentException(
                "policy ["
                    + policy.getName()
                    + "] defines the ["
                    + SearchableSnapshotAction.NAME
                    + "] action but the current license is non-compliant for [searchable-snapshots]"
            );
        }
        // make sure any referenced snapshot repositories exist
        for (Phase phase : phasesWithSearchableSnapshotActions) {
            SearchableSnapshotAction action = (SearchableSnapshotAction) phase.getActions().get(SearchableSnapshotAction.NAME);
            String repository = action.getSnapshotRepository();
            if (RepositoriesMetadata.get(projectMetadata).repository(repository) == null) {
                throw new IllegalArgumentException(
                    "no such repository ["
                        + repository
                        + "], the snapshot repository "
                        + "referenced by the ["
                        + SearchableSnapshotAction.NAME
                        + "] action in the ["
                        + phase.getName()
                        + "] phase "
                        + "must exist before it can be referenced by an ILM policy"
                );
            }
        }

        List<Phase> phasesWithWaitForSnapshotActions = policy.getPhases()
            .values()
            .stream()
            .filter(phase -> phase.getActions().containsKey(WaitForSnapshotAction.NAME))
            .toList();
        // make sure any referenced snapshot lifecycle policies exist
        for (Phase phase : phasesWithWaitForSnapshotActions) {
            WaitForSnapshotAction action = (WaitForSnapshotAction) phase.getActions().get(WaitForSnapshotAction.NAME);
            String slmPolicy = action.getPolicy();
            if (projectMetadata.custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY)
                .getSnapshotConfigurations()
                .get(slmPolicy) == null) {
                throw new IllegalArgumentException(
                    "no such snapshot lifecycle policy ["
                        + slmPolicy
                        + "], the snapshot lifecycle policy "
                        + "referenced by the ["
                        + WaitForSnapshotAction.NAME
                        + "] action in the ["
                        + phase.getName()
                        + "] phase "
                        + "must exist before it can be referenced by an ILM policy"
                );
            }
        }
    }

    public void bulkLifecycleChange(
        BulkLifecycleOperation bulkOp,
        ClusterState state,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final ProjectId projectId = projectResolver.getProjectId();
        ProjectMetadata projectMetadata = state.getMetadata().getProject(projectId);
        validateBatch(bulkOp, projectMetadata);
        var finalBulkOp = filterBatch(bulkOp, projectMetadata);
        if (finalBulkOp.isEmpty()) {
            return;
        }
        var task = new BulkLifecyclePolicyTask(
            projectId,
            finalBulkOp,
            licenseState,
            xContentRegistry,
            client,
            masterNodeTimeout,
            ackTimeout,
            listener
        );
        String source;
        if (bulkOp.requests.size() == 1 && bulkOp.deletePolicies.isEmpty()) {
            source = PUT_SOURCE_PREFIX + bulkOp.requests.iterator().next().getPolicy().getName();
        } else if (bulkOp.requests.isEmpty() && bulkOp.deletePolicies.size() == 1) {
            source = DELETE_SOURCE_PREFIX + bulkOp.deletePolicies.iterator().next();
        } else {
            source = BULK_SOURCE;
        }
        taskQueue.submitTask(source, task, task.timeout());
    }

    public static class BulkLifecyclePolicyTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final BulkLifecycleOperation bulkOp;
        private final XPackLicenseState licenseState;
        private final NamedXContentRegistry xContentRegistry;
        private final Client client;

        public BulkLifecyclePolicyTask(
            ProjectId projectId,
            BulkLifecycleOperation bulkOp,
            XPackLicenseState licenseState,
            NamedXContentRegistry xContentRegistry,
            Client client,
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(Priority.NORMAL, masterNodeTimeout, ackTimeout, listener);
            this.projectId = projectId;
            this.bulkOp = bulkOp;
            this.licenseState = licenseState;
            this.xContentRegistry = xContentRegistry;
            this.client = client;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            var currentProject = currentState.getMetadata().getProject(projectId);
            BulkMetadataOperationContext<Void> ctx = doApplyBatch(bulkOp, currentProject, licenseState, xContentRegistry, client);
            if (ctx == null) {
                assert false : "LifecycleMetadataService returned null context from applyBatch method.";
                return currentState;
            }
            if (ctx.getResult() != currentProject) {
                return ClusterState.builder(currentState).putProjectMetadata(ctx.getResult()).build();
            } else {
                return currentState;
            }
        }
    }

    public static class UpdateLifecyclePolicyTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final PutLifecycleRequest request;
        private final XPackLicenseState licenseState;
        private final NamedXContentRegistry xContentRegistry;
        private final Client client;
        private final boolean verboseLogging;

        /**
         * Used by the {@link ReservedClusterStateHandler} for ILM
         * {@link ReservedLifecycleAction}
         * <p>
         * It disables verbose logging and has no filtered headers.
         */
        public UpdateLifecyclePolicyTask(
            ProjectId projectId,
            PutLifecycleRequest request,
            XPackLicenseState licenseState,
            NamedXContentRegistry xContentRegistry,
            Client client
        ) {
            super(request, null);
            this.projectId = projectId;
            this.request = request;
            this.licenseState = licenseState;
            this.xContentRegistry = xContentRegistry;
            this.client = client;
            this.verboseLogging = false;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            var projectMetadata = currentState.getMetadata().getProject(projectId);
            BulkMetadataOperationContext<Void> result = doApplyBatch(
                BulkLifecycleOperation.singleUpdate(request, verboseLogging),
                projectMetadata,
                licenseState,
                xContentRegistry,
                client
            );
            if (result.getResult() != projectMetadata) {
                return ClusterState.builder(currentState).putProjectMetadata(result.getResult()).build();
            } else {
                return currentState;
            }
        }
    }

    public static class DeleteLifecyclePolicyTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final DeleteLifecycleAction.Request request;

        public DeleteLifecyclePolicyTask(
            ProjectId projectId,
            DeleteLifecycleAction.Request request,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(request, listener);
            this.projectId = projectId;
            this.request = request;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            String policyToDelete = request.getPolicyName();
            ProjectMetadata projectMetadata = currentState.metadata().getProject(projectId);
            List<String> indicesUsingPolicy = projectMetadata.indices()
                .values()
                .stream()
                .filter(idxMeta -> policyToDelete.equals(idxMeta.getLifecyclePolicyName()))
                .map(idxMeta -> idxMeta.getIndex().getName())
                .toList();
            if (indicesUsingPolicy.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Cannot delete policy [" + request.getPolicyName() + "]. It is in use by one or more indices: " + indicesUsingPolicy
                );
            }
            IndexLifecycleMetadata currentMetadata = projectMetadata.custom(IndexLifecycleMetadata.TYPE);
            if (currentMetadata == null || currentMetadata.getPolicyMetadatas().containsKey(request.getPolicyName()) == false) {
                throw new ResourceNotFoundException("Lifecycle policy not found: {}", request.getPolicyName());
            }
            SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());
            newPolicies.remove(request.getPolicyName());
            IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentILMMode(projectMetadata));
            ProjectMetadata.Builder newProjectMetadata = ProjectMetadata.builder(projectMetadata)
                .putCustom(IndexLifecycleMetadata.TYPE, newMetadata);
            return ClusterState.builder(currentState).putProjectMetadata(newProjectMetadata).build();
        }
    }

    private static class IlmLifecycleExecutor extends SimpleBatchedAckListenerTaskExecutor<BulkLifecyclePolicyTask> {
        @Override
        public Tuple<ClusterState, ClusterStateAckListener> executeTask(BulkLifecyclePolicyTask task, ClusterState clusterState)
            throws Exception {
            return Tuple.tuple(task.execute(clusterState), task);
        }
    }
}
