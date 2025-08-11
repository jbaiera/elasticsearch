/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.BulkMetadataService;
import org.elasticsearch.cluster.metadata.BulkMetadataServiceTask;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.ilm.action.ReservedLifecycleAction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

public class PutLifecycleMetadataService implements BulkMetadataService<PutLifecycleMetadataService.BulkPutLifecycleOperation, Void, Void> {

    private static final Logger logger = LogManager.getLogger(PutLifecycleMetadataService.class);

    private static final String BULK_METADATA_SERVICE_NAME = "elasticsearch.xpack.ilm";

    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final ThreadPool threadPool;
    private final ProjectResolver projectResolver;
    private final MasterServiceTaskQueue<BulkUpdateLifecyclePolicyTask> taskQueue;

    public PutLifecycleMetadataService(
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
            new PutLifecycleMetadataService.IlmLifecycleExecutor()
        );
    }

    @Override
    public String getBulkMetadataServiceName() {
        return BULK_METADATA_SERVICE_NAME;
    }

    @Override
    public BulkMetadataServiceTask<BulkPutLifecycleOperation, Void, Void> createTask(
        BulkMetadataOperation op
    ) {
        return new BulkMetadataServiceTask<>(this, (BulkPutLifecycleOperation) op);
    }

    public static class BulkPutLifecycleOperation extends BulkMetadataOperation {
        final List<PutLifecycleRequest> requests;
        final Map<String, String> filteredHeaders;
        final boolean verboseLogging;

        public BulkPutLifecycleOperation(List<PutLifecycleRequest> requests, boolean verboseLogging) {
            this(requests, null, verboseLogging);
        }

        public BulkPutLifecycleOperation(
            List<PutLifecycleRequest> requests,
            Map<String, String> filteredHeaders,
            boolean verboseLogging
        ) {
            super(BULK_METADATA_SERVICE_NAME);
            this.requests = requests;
            this.filteredHeaders = filteredHeaders;
            this.verboseLogging = verboseLogging;
        }

        @Override
        public boolean isEmpty() {
            return requests == null || requests.isEmpty();
        }
    }

    @Override
    public void validateBatch(BulkPutLifecycleOperation batch, ProjectMetadata currentProject) {
        for (PutLifecycleRequest request : batch.requests) {
            LifecyclePolicy.validatePolicyName(request.getPolicy().getName());
            request.getPolicy().maybeAddDeprecationWarningForFreezeAction(request.getPolicy().getName());
        }
    }

    @Override
    public BulkPutLifecycleOperation filterBatch(
        PutLifecycleMetadataService.BulkPutLifecycleOperation batch,
        ProjectMetadata currentProject
    ) {
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
        return new BulkPutLifecycleOperation(filteredRequests, filteredHeaders, batch.verboseLogging);
    }

    @Override
    public BulkMetadataOperationContext<Void> applyBatch(
        PutLifecycleMetadataService.BulkPutLifecycleOperation batch,
        ProjectMetadata previousProject
    ) {
        return doApplyBatch(batch, previousProject, licenseState, xContentRegistry, client);
    }

    private static BulkMetadataOperationContext<Void> doApplyBatch(
        PutLifecycleMetadataService.BulkPutLifecycleOperation batch,
        ProjectMetadata previousProject,
        XPackLicenseState licenseState,
        NamedXContentRegistry xContentRegistry,
        Client client
    ) {
        final IndexLifecycleMetadata currentMetadata = previousProject.custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);

        SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());

        ProjectMetadata.Builder projectBuilder = null;
        boolean updatesToPolicies = false;
        for (PutLifecycleRequest request : batch.requests) {
            final LifecyclePolicyMetadata existingPolicyMetadata = currentMetadata.getPolicyMetadatas().get(request.getPolicy().getName());

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
            updatesToPolicies = true;
            if (projectBuilder == null) {
                projectBuilder = ProjectMetadata.builder(previousProject);
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
        if (updatesToPolicies) {
            IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentILMMode(previousProject));
            projectBuilder.putCustom(IndexLifecycleMetadata.TYPE, newMetadata);
            return BulkMetadataOperationContext.noContext(projectBuilder.build());
        } else {
            return BulkMetadataOperationContext.noContext(previousProject);
        }
    }

    public void addLifecycle(PutLifecycleRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        var bulkOp = new PutLifecycleMetadataService.BulkPutLifecycleOperation(List.of(request), true);
        final ProjectId projectId = projectResolver.getProjectId();
        ProjectMetadata projectMetadata = state.getMetadata().getProject(projectId);
        validateBatch(bulkOp, projectMetadata);
        var finalBulkOp = filterBatch(bulkOp, projectMetadata);
        if (finalBulkOp.isEmpty()) {
            return;
        }
        var putTask = new PutLifecycleMetadataService.BulkUpdateLifecyclePolicyTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                var currentMetadata = currentState.getMetadata().getProject(projectId);
                BulkMetadataOperationContext<Void> ctx = applyBatch(finalBulkOp, currentMetadata);
                if (ctx == null) {
                    assert false : "PutLifecycleMetadataService returned null context from applyBatch method.";
                    return currentState;
                }
                if (ctx.getResult() != currentMetadata) {
                    return ClusterState.builder(currentState).putProjectMetadata(ctx.getResult()).build();
                } else {
                    return currentState;
                }
            }
        };
        taskQueue.submitTask("put-lifecycle-" + request.getPolicy().getName(), putTask, putTask.timeout());
    }

    /**
     * Returns 'true' if the ILM policy is effectually the same (same policy and headers), and thus can be a no-op update.
     */
    static boolean isNoopUpdate(
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

    private abstract static class BulkUpdateLifecyclePolicyTask extends AckedClusterStateUpdateTask {
        protected BulkUpdateLifecyclePolicyTask(PutLifecycleRequest request, ActionListener<AcknowledgedResponse> listener) {
            super(request, listener);
        }
        @Override
        public abstract ClusterState execute(ClusterState currentState) throws Exception;
    }

    public static class UpdateLifecyclePolicyTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final PutLifecycleRequest request;
        private final XPackLicenseState licenseState;
        private final Map<String, String> filteredHeaders;
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
            this.filteredHeaders = Collections.emptyMap();
            this.xContentRegistry = xContentRegistry;
            this.client = client;
            this.verboseLogging = false;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            var projectMetadata = currentState.getMetadata().getProject(projectId);
            BulkMetadataOperationContext<Void> result = doApplyBatch(
                new PutLifecycleMetadataService.BulkPutLifecycleOperation(List.of(request), filteredHeaders, verboseLogging),
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

    private static class IlmLifecycleExecutor extends SimpleBatchedAckListenerTaskExecutor<
        PutLifecycleMetadataService.BulkUpdateLifecyclePolicyTask> {
        @Override
        public Tuple<ClusterState, ClusterStateAckListener> executeTask(
            PutLifecycleMetadataService.BulkUpdateLifecyclePolicyTask task,
            ClusterState clusterState
        )
            throws Exception {
            return Tuple.tuple(task.execute(clusterState), task);
        }
    }
}
