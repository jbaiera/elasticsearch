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
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.validateTimestampFieldMapping;

/**
 * Service responsible for submitting index templates updates
 */
public class MetadataIndexTemplateService {

    public static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";
    public static final CompressedXContent DEFAULT_TIMESTAMP_MAPPING_WITHOUT_ROUTING;
    // Names used for validating templates when we do not know the index or data stream name
    public static final String VALIDATE_INDEX_NAME = "validate-index-name";
    public static final String VALIDATE_DATA_STREAM_NAME = "validate-data-stream-name";

    private static final CompressedXContent DEFAULT_TIMESTAMP_MAPPING_WITH_ROUTING;

    static {
        final Map<String, Map<String, String>> defaultTimestampField = Map.of(
            DEFAULT_TIMESTAMP_FIELD,
            // We inject ignore_malformed false so that if a user does not add the timestamp field it will explicitly skip applying any
            // other ignore_malformed configurations from the index settings.
            Map.of("type", DateFieldMapper.CONTENT_TYPE, "ignore_malformed", "false")
        );
        try {
            DEFAULT_TIMESTAMP_MAPPING_WITHOUT_ROUTING = new CompressedXContent(
                (builder, params) -> builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                    // adding explicit "_routing": {"required": false}, even though this is the default, because this snippet is used
                    // later for resolving a RoutingFieldMapper, where we need this information to validate that does not conflict with
                    // any mapping.
                    .startObject(RoutingFieldMapper.NAME)
                    .field("required", false)
                    .endObject()
                    .field("properties", defaultTimestampField)
                    .endObject()
            );
            DEFAULT_TIMESTAMP_MAPPING_WITH_ROUTING = new CompressedXContent(
                (builder, params) -> builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startObject(RoutingFieldMapper.NAME)
                    .field("required", true)
                    .endObject()
                    .field("properties")
                    .map(defaultTimestampField)
                    .endObject()
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static final Logger logger = LogManager.getLogger(MetadataIndexTemplateService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MetadataIndexTemplateService.class);

    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<TemplateClusterStateUpdateTask> taskQueue;
    private final IndicesService indicesService;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final IndexScopedSettings indexScopedSettings;
    private final NamedXContentRegistry xContentRegistry;
    private final SystemIndices systemIndices;
    private final Set<IndexSettingProvider> indexSettingProviders;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;
    private final InstantSource instantSource;

    /**
     * This is the cluster state task executor for all template-based actions.
     */
    private static final SimpleBatchedExecutor<TemplateClusterStateUpdateTask, Void> TEMPLATE_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(TemplateClusterStateUpdateTask task, ClusterState clusterState) throws Exception {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(TemplateClusterStateUpdateTask task, Void unused) {
                task.listener.onResponse(AcknowledgedResponse.TRUE);
            }
        };

    /**
     * A specialized cluster state update task that always takes a listener handling an
     * AcknowledgedResponse, as all template actions have simple acknowledged yes/no responses.
     */
    private abstract static class TemplateClusterStateUpdateTask implements ClusterStateTaskListener {
        final ActionListener<AcknowledgedResponse> listener;
        final ProjectId projectId;

        TemplateClusterStateUpdateTask(ActionListener<AcknowledgedResponse> listener, ProjectId projectId) {
            this.listener = listener;
            this.projectId = projectId;
        }

        public final ClusterState execute(ClusterState currentState) throws Exception {
            ProjectMetadata currentProject = currentState.metadata().getProject(projectId);
            ProjectMetadata newProject = execute(currentProject);
            if (currentProject == newProject) {
                return currentState;
            }
            return ClusterState.builder(currentState).metadata(currentState.metadata().withUpdatedProject(newProject)).build();
        }

        public abstract ProjectMetadata execute(ProjectMetadata currentProject) throws Exception;

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    public MetadataIndexTemplateService(
        ClusterService clusterService,
        MetadataCreateIndexService metadataCreateIndexService,
        IndicesService indicesService,
        IndexScopedSettings indexScopedSettings,
        NamedXContentRegistry xContentRegistry,
        SystemIndices systemIndices,
        IndexSettingProviders indexSettingProviders,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        this(
            clusterService,
            metadataCreateIndexService,
            indicesService,
            indexScopedSettings,
            xContentRegistry,
            systemIndices,
            indexSettingProviders,
            globalRetentionSettings,
            Instant::now
        );
    }

    // constructor allowing for injection of InstantSource/time for testing
    MetadataIndexTemplateService(
        ClusterService clusterService,
        MetadataCreateIndexService metadataCreateIndexService,
        IndicesService indicesService,
        IndexScopedSettings indexScopedSettings,
        NamedXContentRegistry xContentRegistry,
        SystemIndices systemIndices,
        IndexSettingProviders indexSettingProviders,
        DataStreamGlobalRetentionSettings globalRetentionSettings,
        InstantSource instantSource
    ) {
        this.clusterService = clusterService;
        this.taskQueue = clusterService.createTaskQueue("index-templates", Priority.URGENT, TEMPLATE_TASK_EXECUTOR);
        this.indicesService = indicesService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.indexScopedSettings = indexScopedSettings;
        this.xContentRegistry = xContentRegistry;
        this.systemIndices = systemIndices;
        this.indexSettingProviders = indexSettingProviders.getIndexSettingProviders();
        this.globalRetentionSettings = globalRetentionSettings;
        this.instantSource = instantSource;
    }

    public void removeTemplates(
        final ProjectId projectId,
        final String templatePattern,
        final TimeValue timeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask("remove-index-template [" + templatePattern + "]", new TemplateClusterStateUpdateTask(listener, projectId) {
            @Override
            public ProjectMetadata execute(ProjectMetadata project) {
                Set<String> templateNames = new HashSet<>();
                for (Map.Entry<String, IndexTemplateMetadata> cursor : project.templates().entrySet()) {
                    String templateName = cursor.getKey();
                    if (Regex.simpleMatch(templatePattern, templateName)) {
                        templateNames.add(templateName);
                    }
                }
                if (templateNames.isEmpty()) {
                    // if its a match all pattern, and no templates are found (we have none), don't
                    // fail with index missing...
                    if (Regex.isMatchAllPattern(templatePattern)) {
                        return project;
                    }
                    throw new IndexTemplateMissingException(templatePattern);
                }
                ProjectMetadata.Builder metadata = ProjectMetadata.builder(project);
                for (String templateName : templateNames) {
                    logger.info("removing template [{}]", templateName);
                    metadata.removeTemplate(templateName);
                }
                return metadata.build();
            }
        }, timeout);
    }

    /**
     * Add the given component template to the cluster state. If {@code create} is true, an
     * exception will be thrown if the component template already exists
     */
    public void putComponentTemplate(
        final String cause,
        final boolean create,
        final String name,
        final TimeValue masterTimeout,
        final ComponentTemplate template,
        final ProjectId projectId,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask(
            "create-component-template [" + name + "], cause [" + cause + "]",
            new TemplateClusterStateUpdateTask(listener, projectId) {
                @Override
                public ProjectMetadata execute(ProjectMetadata currentProject) throws Exception {
                    return addComponentTemplate(currentProject, create, name, template);
                }
            },
            masterTimeout
        );
    }

    /**
     * A simple request holder for component template updates
     * @param name
     * @param template
     * @param create
     */
    private record ComponentTemplateOperation(String name, ComponentTemplate template, boolean create) {}

    /**
     * A simple request holder for index template updates
     * @param name
     * @param template
     * @param create
     */
    private record ComposableTemplateOperation(String name, ComposableIndexTemplate template, boolean create, boolean validateV2Overlaps) {}

    /**
     * A name/template tuple record
     * @param name of template
     * @param template definition
     */
    private record NamedTemplateTuple(String name, ComposableIndexTemplate template) {}

    private ProjectMetadata processBulkTemplateUpdate(
        final ProjectMetadata currentProject,
        final Map<String, ComponentTemplateOperation> componentTemplateOperations,
        final Map<String, ComposableTemplateOperation> composableTemplateOperations
    ) throws Exception {
        Map<String, ComponentTemplate> intialComponentTemplates = currentProject.componentTemplates();
        Map<String, ComposableIndexTemplate> initialTemplatesV2 = currentProject.templatesV2();

        Map<String, ComponentTemplate> updatedComponentTemplates = new HashMap<>();
        Map<String, ComposableIndexTemplate> updatedComposableIndexTemplates = new HashMap<>();

        // Keep a union of current and updated composable index templates to represent the updated cluster state contents
        Map<String, ComponentTemplate> workingComponents = new HashMap<>(intialComponentTemplates);
        Map<String, ComposableIndexTemplate> workingTemplatesV2 = new HashMap<>(initialTemplatesV2);

        // Process and finalize all templates to be operated on
        for (ComponentTemplateOperation componentTemplate : componentTemplateOperations.values()) {
            final ComponentTemplate maybeFinalComponentTemplate = prepareComponentTemplate(
                intialComponentTemplates,
                componentTemplate.create,
                componentTemplate.name,
                componentTemplate.template
            );
            if (maybeFinalComponentTemplate != null) {
                updatedComponentTemplates.put(componentTemplate.name, maybeFinalComponentTemplate);
                workingComponents.put(componentTemplate.name, maybeFinalComponentTemplate);
            }
        }

        for (ComposableTemplateOperation composableIndexTemplate : composableTemplateOperations.values()) {
            final ComposableIndexTemplate maybeFinalComposableTemplate = prepareComposableTemplate(
                currentProject,
                initialTemplatesV2,
                composableIndexTemplate.create,
                composableIndexTemplate.name,
                composableIndexTemplate.template
            );
            if (maybeFinalComposableTemplate != null) {
                updatedComposableIndexTemplates.put(composableIndexTemplate.name, maybeFinalComposableTemplate);
                workingTemplatesV2.put(composableIndexTemplate.name, maybeFinalComposableTemplate);
            }
        }

        if (updatedComponentTemplates.isEmpty() && updatedComposableIndexTemplates.isEmpty()) {
            return currentProject;
        }

        // Collect all the composable index templates that use any of the updated component template. Additionally, collect all updated
        // component templates that were used by any index template. We'll use both of these for validating that both sets of templates
        // are (still) valid after updating.
        final Map<String, ComposableIndexTemplate> allTemplatesRequiringValidation = new HashMap<>();
        final Map<String, List<NamedTemplateTuple>> updatedComponentsUsedByTemplates = new HashMap<>();
        for (Map.Entry<String, ComposableIndexTemplate> workingTemplateV2 : workingTemplatesV2.entrySet()) {
            String indexTemplateName = workingTemplateV2.getKey();
            ComposableIndexTemplate indexTemplateDefinition = workingTemplateV2.getValue();
            boolean usesUpdatedComponents = false;

            // Check each component dependency to see if it was updated during this bulk operation
            for (String component : indexTemplateDefinition.composedOf()) {
                if (updatedComponentTemplates.containsKey(component)) {
                    usesUpdatedComponents = true;
                    // Capture this updated component's relationship with this index template
                    updatedComponentsUsedByTemplates.compute(component, (key, value) -> {
                        List<NamedTemplateTuple> values = value == null ? new ArrayList<>() : value;
                        values.add(new NamedTemplateTuple(indexTemplateName, indexTemplateDefinition));
                        return values;
                    });
                }
            }

            // Capture all index templates that need to validate their composition
            if (usesUpdatedComponents || updatedComposableIndexTemplates.containsKey(indexTemplateName)) {
                allTemplatesRequiringValidation.put(indexTemplateName, indexTemplateDefinition);
            }
        }

        // Validate all the templates to make sure they aren't adding hidden index settings to global patterns
        for (Map.Entry<String, ComposableIndexTemplate> updatedComposableIndexTemplate : updatedComposableIndexTemplates.entrySet()) {
            String indexTemplateName = updatedComposableIndexTemplate.getKey();
            ComposableIndexTemplate composableIndexTemplate = updatedComposableIndexTemplate.getValue();
            boolean validateV2Overlaps = composableTemplateOperations.get(indexTemplateName).validateV2Overlaps();

            MetadataIndexTemplateService.validateV2TemplateRequest(workingComponents, indexTemplateName, composableIndexTemplate);
            MetadataIndexTemplateService.v2TemplateOverlaps(
                workingTemplatesV2,
                indexTemplateName,
                composableIndexTemplate,
                validateV2Overlaps
            );
        }
        // Do the same validation for the component template changes
        // PRTODO: Is this redundant with the validateV2TemplateRequest call in the block right before here?
        for (Map.Entry<String, List<NamedTemplateTuple>> updatedComponentUsedByTemplate : updatedComponentsUsedByTemplates.entrySet()) {
            String componentTemplateName = updatedComponentUsedByTemplate.getKey();
            List<NamedTemplateTuple> composableTemplatesUsingThisComponent = updatedComponentUsedByTemplate.getValue();

            ComponentTemplate componentTemplate = updatedComponentTemplates.get(componentTemplateName);
            validateNoHiddenSettingOnGlobalTemplates(componentTemplateName, componentTemplate, composableTemplatesUsingThisComponent);
        }

        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(currentProject);

        // Sufficiently validated enough to apply changes to a candidate cluster state to complete the rest of the validation
        for (Map.Entry<String, ComponentTemplate> component : updatedComponentTemplates.entrySet()) {
            projectBuilder.put(component.getKey(), component.getValue());
        }
        for (Map.Entry<String, ComposableIndexTemplate> indexTemplate : updatedComposableIndexTemplates.entrySet()) {
            projectBuilder.put(indexTemplate.getKey(), indexTemplate.getValue());
        }

        ProjectMetadata candidateProject = projectBuilder.build();

        // Validate all changed composable index templates that have been updated, either directly or by changes to their dependencies
        if (allTemplatesRequiringValidation.isEmpty() == false) {
            Exception validationFailure = null;
            for (Map.Entry<String, ComposableIndexTemplate> entry : allTemplatesRequiringValidation.entrySet()) {
                final String composableTemplateName = entry.getKey();
                final ComposableIndexTemplate composableTemplate = entry.getValue();
                // PRTODO: This error matches what is expected for component template changes, but not for composable template changes
                try {
                    validateIndexTemplateV2(
                        candidateProject,
                        composableTemplateName,
                        composableTemplate
                    );
                } catch (Exception e) {
                    // For the sake of error message backwards compatibility, do not wrap the
                    // exception if this is a single composable index template updated
                    if (composableTemplateOperations.size() == 1 && componentTemplateOperations.isEmpty()) {
                        throw e;
                    }
                    if (validationFailure == null) {
                        validationFailure = new IllegalArgumentException(
                            "updating templates ["
                                + generateTemplateNamesForException(
                                    composableTemplateName,
                                    composableTemplate,
                                    updatedComponentTemplates,
                                    updatedComposableIndexTemplates
                                )
                                + "] results in invalid composable template ["
                                + composableTemplateName
                                + "] after templates are merged",
                            e
                        );
                    } else {
                        validationFailure.addSuppressed(e);
                    }
                }
            }
            if (validationFailure != null) {
                throw validationFailure;
            }
        }

        validateDataStreamsStillReferenced(candidateProject, currentProject, updatedComposableIndexTemplates.keySet());

        return candidateProject;
    }

    private static StringBuilder generateTemplateNamesForException(
        String composableTemplateName,
        ComposableIndexTemplate composableTemplate,
        Map<String, ComponentTemplate> updatedComponentTemplates,
        Map<String, ComposableIndexTemplate> updatedComposableIndexTemplates
    ) {
        StringBuilder builder = new StringBuilder();
        var failingComponents = composableTemplate.composedOf()
            .stream()
            .filter(updatedComponentTemplates::containsKey)
            .collect(Collectors.joining(", "));
        if (failingComponents.isEmpty() == false) {
            builder.append(failingComponents);
        }
        var failingTemplate = updatedComposableIndexTemplates.containsKey(composableTemplateName)
            ? composableTemplateName
            : null;
        if (failingTemplate != null) {
            if (failingComponents.isEmpty() == false) {
                builder.append(", ");
            }
            builder.append(failingTemplate);
        }
        return builder;
    }

    /**
     * Validate that a component template does not set an index to hidden if it is used within a global template (match *)
     * @param name of the component template to check
     * @param componentTemplate definition of the template to check
     * @param templatesUsingComponent all index templates that currently make use of this component
     */
    private static void validateNoHiddenSettingOnGlobalTemplates(
        String name,
        ComponentTemplate componentTemplate,
        List<NamedTemplateTuple> templatesUsingComponent
    ) {

        Settings finalSettings = Optional.of(componentTemplate).map(ComponentTemplate::template).map(Template::settings).orElse(null);
        if (finalSettings != null) {
            // if the CT is specifying the `index.hidden` setting it cannot be part of any global template
            if (IndexMetadata.INDEX_HIDDEN_SETTING.exists(finalSettings)) {
                List<String> globalTemplatesThatUseThisComponent = new ArrayList<>();
                for (NamedTemplateTuple template : templatesUsingComponent) {
                    ComposableIndexTemplate templateV2 = template.template;
                    if (templateV2.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)) {
                        // global templates don't support configuring the `index.hidden` setting so we don't need to resolve the settings as
                        // no other component template can remove this setting from the resolved settings, so just invalidate this update
                        globalTemplatesThatUseThisComponent.add(template.name);
                    }
                }
                if (globalTemplatesThatUseThisComponent.isEmpty() == false) {
                    throw new IllegalArgumentException(
                        "cannot update component template ["
                            + name
                            + "] because the following global templates would resolve to specifying the ["
                            + IndexMetadata.SETTING_INDEX_HIDDEN
                            + "] setting: ["
                            + String.join(",", globalTemplatesThatUseThisComponent)
                            + "]"
                    );
                }
            }
        }
    }

    private ComponentTemplate prepareComponentTemplate(
        Map<String, ComponentTemplate> intialComponentTemplates,
        boolean create,
        String name,
        ComponentTemplate template
    ) throws Exception {
        final ComponentTemplate existing = intialComponentTemplates.get(name);
        if (create && existing != null) {
            throw new IllegalArgumentException("component template [" + name + "] already exists");
        }

        CompressedXContent mappings = template.template().mappings();
        CompressedXContent wrappedMappings = MetadataIndexTemplateService.wrapMappingsIfNecessary(mappings, xContentRegistry);

        // We may need to normalize index settings, so do that also
        Settings finalSettings = template.template().settings();
        if (finalSettings != null) {
            finalSettings = Settings.builder().put(finalSettings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        }

        final Template finalTemplate = Template.builder(template.template()).settings(finalSettings).mappings(wrappedMappings).build();
        final long now = instantSource.instant().toEpochMilli();
        final ComponentTemplate finalComponentTemplate;
        if (existing == null) {
            finalComponentTemplate = new ComponentTemplate(
                finalTemplate,
                template.version(),
                template.metadata(),
                template.deprecated(),
                now,
                now
            );
        } else {
            final ComponentTemplate templateToCompareToExisting = new ComponentTemplate(
                finalTemplate,
                template.version(),
                template.metadata(),
                template.deprecated(),
                existing.createdDateMillis().orElse(null),
                existing.modifiedDateMillis().orElse(null)
            );
            if (templateToCompareToExisting.equals(existing)) {
                return null;
            }
            finalComponentTemplate = new ComponentTemplate(
                finalTemplate,
                template.version(),
                template.metadata(),
                template.deprecated(),
                existing.createdDateMillis().orElse(null),
                now
            );
        }

        // Immediate validation of the component template only
        validateTemplate(finalSettings, wrappedMappings, indicesService);
        validate(name, finalComponentTemplate.template(), List.of(), null);
        if (finalComponentTemplate.template().lifecycle() != null) {
            // We do not know if this lifecycle will belong to an internal data stream, so we fall back to a non internal.
            finalComponentTemplate.template()
                .lifecycle()
                .toDataStreamLifecycle()
                .addWarningHeaderIfDataRetentionNotEffective(globalRetentionSettings.get(false), false);
        }
        return finalComponentTemplate;
    }

    private ComposableIndexTemplate prepareComposableTemplate(
        final ProjectMetadata previousProject,
        final Map<String, ComposableIndexTemplate> initialTemplatesV2,
        final boolean create,
        final String name,
        final ComposableIndexTemplate template
    ) throws IOException {
        final ComposableIndexTemplate existing = initialTemplatesV2.get(name);
        if (create && existing != null) {
            throw new IllegalArgumentException("index template [" + name + "] already exists");
        }

        Map<String, List<String>> overlaps = MetadataIndexTemplateService.findConflictingV1Templates(
            previousProject,
            name,
            template.indexPatterns()
        );
        if (overlaps.size() > 0) {
            String warning = String.format(
                Locale.ROOT,
                "index template [%s] has index patterns %s matching patterns from "
                    + "existing older templates [%s] with patterns (%s); this template [%s] will take precedence during new index creation",
                name,
                template.indexPatterns(),
                Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                overlaps.entrySet().stream().map(e -> e.getKey() + " => " + e.getValue()).collect(Collectors.joining(",")),
                name
            );
            logger.warn(warning);
            HeaderWarning.addWarning(warning);
        }

        // Normalize the internal template part of this component template if it is present
        final ComposableIndexTemplate.Builder finalIndexTemplateBuilder = template.toBuilder();
        final Template innerTemplate = template.template();
        if (innerTemplate != null) {
            // We may need to normalize index settings, so do that also
            Settings finalSettings = innerTemplate.settings();
            if (finalSettings != null) {
                finalSettings = Settings.builder().put(finalSettings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
            }
            // If an inner template was specified, its mappings may need to be
            // adjusted (to add _doc) and it should be validated
            final CompressedXContent mappings = innerTemplate.mappings();
            final CompressedXContent wrappedMappings = MetadataIndexTemplateService.wrapMappingsIfNecessary(mappings, xContentRegistry);
            final Template finalTemplate = Template.builder(innerTemplate).settings(finalSettings).mappings(wrappedMappings).build();
            finalIndexTemplateBuilder.template(finalTemplate);
        }

        final long now = instantSource.millis();
        final ComposableIndexTemplate finalIndexTemplate;
        if (existing == null) {
            finalIndexTemplate = finalIndexTemplateBuilder.createdDate(now).modifiedDate(now).build();
        } else {
            final ComposableIndexTemplate templateToCompareToExisting = finalIndexTemplateBuilder.createdDate(
                existing.createdDateMillis().orElse(null)
            ).modifiedDate(existing.modifiedDateMillis().orElse(null)).build();

            if (templateToCompareToExisting.equals(existing)) {
                return null;
            }
            finalIndexTemplate = finalIndexTemplateBuilder.modifiedDate(now).build();
        }

        // If this finalized template hasn't changed compared to what is in the cluster, skip updating it
        if (finalIndexTemplate.equals(existing)) {
            return null;
        }
        return finalIndexTemplate;
    }

    // Public visible for testing
    public ProjectMetadata addComponentTemplate(
        final ProjectMetadata currentProject,
        final boolean create,
        final String name,
        final ComponentTemplate template
    ) throws Exception {
        ProjectMetadata newProject = processBulkTemplateUpdate(
            currentProject,
            Map.of(name, new ComponentTemplateOperation(name, template, create)),
            Map.of()
        );
        logger.info(
            "{} component template [{}]",
            currentProject.componentTemplates().containsKey(name) ? "updating" : "adding",
            name
        );
        return newProject;
    }

    /**
     * Mappings in templates don't have to include <code>_doc</code>, so update the mappings to include this single type if necessary
     *
     * @param mappings mappings from a template
     * @param xContentRegistry the xcontent registry used for parsing
     * @return a normalized form of the mapping provided
     * @throws IOException if reading or writing the mapping encounters a problem
     */
    @Nullable
    public static CompressedXContent wrapMappingsIfNecessary(@Nullable CompressedXContent mappings, NamedXContentRegistry xContentRegistry)
        throws IOException {
        // Mappings in templates don't have to include _doc, so update
        // the mappings to include this single type if necessary

        CompressedXContent wrapped = mappings;
        if (wrapped != null) {
            Map<String, Object> parsedMappings = MapperService.parseMapping(xContentRegistry, mappings);
            if (parsedMappings.size() > 0) {
                if (parsedMappings.size() == 1) {
                    final String keyName = parsedMappings.keySet().iterator().next();
                    // Check if it's already wrapped in `_doc`, only rewrap if needed
                    if (MapperService.SINGLE_MAPPING_NAME.equals(keyName) == false) {
                        wrapped = new CompressedXContent(
                            (builder, params) -> builder.field(MapperService.SINGLE_MAPPING_NAME, parsedMappings)
                        );
                    }
                } else {
                    wrapped = new CompressedXContent((builder, params) -> builder.field(MapperService.SINGLE_MAPPING_NAME, parsedMappings));
                }
            }
        }
        return wrapped;
    }

    /**
     * Remove the given component template from the cluster state. The component template name
     * supports simple regex wildcards for removing multiple component templates at a time.
     */
    public void removeComponentTemplate(
        final String[] names,
        final TimeValue masterTimeout,
        final ProjectMetadata project,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        validateCanBeRemoved(project, names);
        taskQueue.submitTask(
            "remove-component-template [" + String.join(",", names) + "]",
            new TemplateClusterStateUpdateTask(listener, project.id()) {
                @Override
                public ProjectMetadata execute(ProjectMetadata currentProject) {
                    return innerRemoveComponentTemplate(currentProject, names);
                }
            },
            masterTimeout
        );
    }

    // Exposed for ReservedComponentTemplateAction
    public static ProjectMetadata innerRemoveComponentTemplate(ProjectMetadata project, String... names) {
        validateCanBeRemoved(project, names);

        final Set<String> templateNames = new HashSet<>();
        if (names.length > 1) {
            Set<String> missingNames = null;
            for (String name : names) {
                if (project.componentTemplates().containsKey(name)) {
                    templateNames.add(name);
                } else {
                    // wildcards are not supported, so if a name with a wildcard is specified then
                    // the else clause gets executed, because template names can't contain a wildcard.
                    if (missingNames == null) {
                        missingNames = new LinkedHashSet<>();
                    }
                    missingNames.add(name);
                }
            }

            if (missingNames != null) {
                throw new ResourceNotFoundException(String.join(",", missingNames));
            }
        } else {
            for (String templateName : project.componentTemplates().keySet()) {
                if (Regex.simpleMatch(names[0], templateName)) {
                    templateNames.add(templateName);
                }
            }
            if (templateNames.isEmpty()) {
                // if its a match all pattern, and no templates are found (we have none), don't
                // fail with index missing...
                if (Regex.isMatchAllPattern(names[0])) {
                    return project;
                }
                throw new ResourceNotFoundException(names[0]);
            }
        }
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);
        for (String templateName : templateNames) {
            logger.info("removing component template [{}]", templateName);
            builder.removeComponentTemplate(templateName);
        }
        return builder.build();
    }

    /**
     * Validates that the given component template can be removed, throwing an error if it cannot.
     * A component template should not be removed if it is <b>required</b> by any index templates,
     * that is- it is used AND NOT specified as {@code ignore_missing_component_templates}.
     */
    static void validateCanBeRemoved(ProjectMetadata project, String... templateNameOrWildcard) {
        final Predicate<String> predicate;
        if (templateNameOrWildcard.length > 1) {
            predicate = name -> Arrays.asList(templateNameOrWildcard).contains(name);
        } else {
            predicate = name -> Regex.simpleMatch(templateNameOrWildcard[0], name);
        }
        final Set<String> matchingComponentTemplates = project.componentTemplates()
            .keySet()
            .stream()
            .filter(predicate)
            .collect(Collectors.toSet());
        final Set<String> componentsBeingUsed = new HashSet<>();
        final List<String> templatesStillUsing = project.templatesV2().entrySet().stream().filter(e -> {
            Set<String> intersecting = Sets.intersection(
                new HashSet<>(e.getValue().getRequiredComponentTemplates()),
                matchingComponentTemplates
            );
            if (intersecting.size() > 0) {
                componentsBeingUsed.addAll(intersecting);
                return true;
            }
            return false;
        }).map(Map.Entry::getKey).toList();

        if (templatesStillUsing.size() > 0) {
            throw new IllegalArgumentException(
                "component templates "
                    + componentsBeingUsed
                    + " cannot be removed as they are still in use by index templates "
                    + templatesStillUsing
            );
        }
    }

    /**
     * Add the given index template to the cluster state. If {@code create} is true, an
     * exception will be thrown if the component template already exists
     */
    public void putIndexTemplateV2(
        final String cause,
        final boolean create,
        final String name,
        final TimeValue masterTimeout,
        final ComposableIndexTemplate template,
        final ProjectId projectId,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        var project = clusterService.state().metadata().getProject(projectId);
        validateV2TemplateRequest(project, name, template);
        taskQueue.submitTask(
            "create-index-template-v2 [" + name + "], cause [" + cause + "]",
            new TemplateClusterStateUpdateTask(listener, projectId) {
                @Override
                public ProjectMetadata execute(ProjectMetadata currentProject) throws Exception {
                    return addIndexTemplateV2(currentProject, create, name, template);
                }
            },
            masterTimeout
        );
    }

    public static void validateV2TemplateRequest(ProjectMetadata metadata, String name, ComposableIndexTemplate template) {
        validateV2TemplateRequest(metadata.componentTemplates(), name, template);
    }

    public static void validateV2TemplateRequest(
        Map<String, ComponentTemplate> componentTemplates,
        String name,
        ComposableIndexTemplate template
    ) {
        if (template.createdDateMillis().isPresent()) {
            throw new InvalidIndexTemplateException(name, "provided a template property which is managed by the system: created_date");
        }
        if (template.modifiedDateMillis().isPresent()) {
            throw new InvalidIndexTemplateException(name, "provided a template property which is managed by the system: modified_date");
        }
        if (template.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)) {
            Settings mergedSettings = resolveSettings(template, componentTemplates);
            if (IndexMetadata.INDEX_HIDDEN_SETTING.exists(mergedSettings)) {
                throw new InvalidIndexTemplateException(
                    name,
                    "global composable templates may not specify the setting " + IndexMetadata.INDEX_HIDDEN_SETTING.getKey()
                );
            }
        }

        final List<String> ignoreMissingComponentTemplates = (template.getIgnoreMissingComponentTemplates() == null
            ? List.of()
            : template.getIgnoreMissingComponentTemplates());
        final List<String> missingComponentTemplates = template.composedOf()
            .stream()
            .filter(componentTemplate -> componentTemplates.containsKey(componentTemplate) == false)
            .filter(componentTemplate -> ignoreMissingComponentTemplates.contains(componentTemplate) == false)
            .toList();

        if (missingComponentTemplates.size() > 0 && ignoreMissingComponentTemplates.size() == 0) {
            throw new InvalidIndexTemplateException(
                name,
                "index template [" + name + "] specifies component templates " + missingComponentTemplates + " that do not exist"
            );
        }

        if (missingComponentTemplates.size() > 0 && ignoreMissingComponentTemplates.size() > 0) {

            throw new InvalidIndexTemplateException(
                name,
                "index template ["
                    + name
                    + "] specifies a missing component templates "
                    + missingComponentTemplates
                    + " "
                    + "that does not exist and is not part of 'ignore_missing_component_templates'"
            );
        }
    }

    public ProjectMetadata addIndexTemplateV2(
        final ProjectMetadata project,
        final boolean create,
        final String name,
        final ComposableIndexTemplate template
    ) throws Exception {
        return addIndexTemplateV2(project, create, name, template, true);
    }

    public ProjectMetadata addIndexTemplateV2(
        final ProjectMetadata project,
        final boolean create,
        final String name,
        final ComposableIndexTemplate template,
        final boolean validateV2Overlaps
    ) throws Exception {
        ProjectMetadata newProject = processBulkTemplateUpdate(
            project,
            Map.of(),
            Map.of(name, new ComposableTemplateOperation(name, template, create, validateV2Overlaps))
        );
        logger.info(
            "{} index template [{}] for index patterns {}",
            project.templatesV2().containsKey(name) ? "updating" : "adding",
            name,
            template.indexPatterns()
        );
        return newProject;
    }

    /**
     * Calculates the conflicting v2 index template overlaps for a given composable index template. Optionally if validate is true
     * we throw an {@link IllegalArgumentException} with information about the conflicting templates.
     * <p>
     * This method doesn't check for conflicting overlaps with v1 templates.
     * @param templatesV2 the templates to check overlap against
     * @param name the composable index template name
     * @param template the full composable index template object we check for overlaps
     * @param validate should we throw {@link IllegalArgumentException} if conflicts are found or just compute them
     * @return a map of v2 template names to their index patterns for v2 templates that would overlap with the given template
     */
    public static Map<String, List<String>> v2TemplateOverlaps(
        Map<String, ComposableIndexTemplate> templatesV2,
        String name,
        final ComposableIndexTemplate template,
        boolean validate
    ) {
        Map<String, List<String>> overlaps = findConflictingV2Templates(
            templatesV2,
            name,
            template.indexPatterns(),
            true,
            template.priorityOrZero()
        );
        overlaps.remove(name);
        if (validate && overlaps.size() > 0) {
            String error = String.format(
                Locale.ROOT,
                "index template [%s] has index patterns %s matching patterns from "
                    + "existing templates [%s] with patterns (%s) that have the same priority [%d], multiple index templates may not "
                    + "match during index creation, please use a different priority",
                name,
                template.indexPatterns(),
                Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                overlaps.entrySet().stream().map(e -> e.getKey() + " => " + e.getValue()).collect(Collectors.joining(",")),
                template.priorityOrZero()
            );
            throw new IllegalArgumentException(error);
        }

        return overlaps;
    }

    // Visibility for testing
    void validateIndexTemplateV2(ProjectMetadata projectMetadata, String name, ComposableIndexTemplate indexTemplate) {
        // Workaround for the fact that start_time and end_time are injected by the MetadataCreateDataStreamService upon creation,
        // but when validating templates that create data streams the MetadataCreateDataStreamService isn't used.
        var finalTemplate = indexTemplate.template();
        final var now = instantSource.instant();

        final var componentTemplates = projectMetadata.componentTemplates();
        final var combinedMappings = collectMappings(indexTemplate, componentTemplates, "tmp_idx");
        final var combinedSettings = resolveSettings(indexTemplate, componentTemplates);
        var additionalSettingsBuilder = Settings.builder();
        for (var provider : indexSettingProviders) {
            Settings.Builder builder = Settings.builder();
            provider.provideAdditionalSettings(
                VALIDATE_INDEX_NAME,
                indexTemplate.getDataStreamTemplate() != null ? VALIDATE_DATA_STREAM_NAME : null,
                projectMetadata.retrieveIndexModeFromTemplate(indexTemplate),
                projectMetadata,
                now,
                combinedSettings,
                combinedMappings,
                IndexVersion.current(),
                builder
            );
            var newAdditionalSettings = builder.build();
            MetadataCreateIndexService.validateAdditionalSettings(provider, newAdditionalSettings, additionalSettingsBuilder);
            additionalSettingsBuilder.put(newAdditionalSettings);
        }
        Settings additionalSettings = additionalSettingsBuilder.build();
        var finalSettings = Settings.builder();
        // First apply settings sourced from index setting providers:
        finalSettings.put(additionalSettings);
        // Then apply setting from component templates:
        finalSettings.put(combinedSettings);
        // Then finally apply settings resolved from index template:
        if (finalTemplate != null && finalTemplate.settings() != null) {
            finalSettings.put(finalTemplate.settings());
        }

        var templateToValidate = indexTemplate.toBuilder().template(Template.builder(finalTemplate).settings(finalSettings)).build();

        validate(name, templateToValidate, additionalSettings);
        validateLifecycle(componentTemplates, name, templateToValidate, globalRetentionSettings.get(false));
        validateDataStreamOptions(componentTemplates, name, templateToValidate, globalRetentionSettings.get(true));

        if (templateToValidate.isDeprecated() == false) {
            validateUseOfDeprecatedComponentTemplates(name, templateToValidate, componentTemplates);
            validateUseOfDeprecatedIngestPipelines(name, projectMetadata.custom(IngestMetadata.TYPE), combinedSettings);
            // TODO come up with a plan how to validate usage of deprecated ILM policies
            // we don't have access to the core/main plugin here so we can't use the IndexLifecycleMetadata type
        }

        // Finally, right before adding the template, we need to ensure that the composite settings,
        // mappings, and aliases are valid after it's been composed with the component templates
        try {
            validateCompositeTemplate(projectMetadata, name, templateToValidate, indicesService, xContentRegistry, systemIndices);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "composable template ["
                    + name
                    + "] template after composition "
                    + (indexTemplate.composedOf().size() > 0 ? "with component templates " + indexTemplate.composedOf() + " " : "")
                    + "is invalid",
                e
            );
        }
    }

    private void validateUseOfDeprecatedComponentTemplates(
        String name,
        ComposableIndexTemplate template,
        Map<String, ComponentTemplate> componentTemplates
    ) {
        template.composedOf()
            .stream()
            .map(ct -> Tuple.tuple(ct, componentTemplates.get(ct)))
            .filter(ct -> Objects.nonNull(ct.v2()))
            .filter(ct -> ct.v2().isDeprecated())
            .forEach(
                ct -> deprecationLogger.warn(
                    DeprecationCategory.TEMPLATES,
                    "use_of_deprecated_component_template",
                    "index template [{}] uses deprecated component template [{}]",
                    name,
                    ct.v1()
                )
            );
    }

    private void validateUseOfDeprecatedIngestPipelines(String name, IngestMetadata ingestMetadata, Settings combinedSettings) {
        Map<String, PipelineConfiguration> pipelines = Optional.ofNullable(ingestMetadata)
            .map(IngestMetadata::getPipelines)
            .orElse(Map.of());
        emitWarningIfPipelineIsDeprecated(name, pipelines, combinedSettings.get("index.default_pipeline"));
        emitWarningIfPipelineIsDeprecated(name, pipelines, combinedSettings.get("index.final_pipeline"));
    }

    private void emitWarningIfPipelineIsDeprecated(String name, Map<String, PipelineConfiguration> pipelines, String pipelineName) {
        Optional.ofNullable(pipelineName)
            .map(pipelines::get)
            .filter(p -> Boolean.TRUE.equals(p.getConfig().get("deprecated")))
            .ifPresent(
                p -> deprecationLogger.warn(
                    DeprecationCategory.TEMPLATES,
                    "use_of_deprecated_ingest_pipeline",
                    "index template [{}] uses deprecated ingest pipeline [{}]",
                    name,
                    p.getId()
                )
            );
    }

    // Visible for testing
    static void validateLifecycle(
        Map<String, ComponentTemplate> componentTemplates,
        String indexTemplateName,
        ComposableIndexTemplate template,
        @Nullable DataStreamGlobalRetention globalRetention
    ) {
        DataStreamLifecycle.Builder builder = resolveLifecycle(template, componentTemplates);
        if (builder != null) {
            if (template.getDataStreamTemplate() == null) {
                throw new IllegalArgumentException(
                    "index template ["
                        + indexTemplateName
                        + "] specifies lifecycle configuration that can only be used in combination with a data stream"
                );
            }
            if (globalRetention != null) {
                // We cannot know for sure if the template will apply to internal data streams, so we use a simpler heuristic:
                // If all the index patterns start with a dot, we consider that all the connected data streams are internal.
                boolean isInternalDataStream = template.indexPatterns().stream().allMatch(indexPattern -> indexPattern.charAt(0) == '.');
                builder.build().addWarningHeaderIfDataRetentionNotEffective(globalRetention, isInternalDataStream);
            }
        }
    }

    // Visible for testing
    static void validateDataStreamOptions(
        Map<String, ComponentTemplate> componentTemplates,
        String indexTemplateName,
        ComposableIndexTemplate template,
        DataStreamGlobalRetention globalRetention
    ) {
        DataStreamOptions.Builder dataStreamOptionsBuilder = resolveDataStreamOptions(template, componentTemplates);
        if (dataStreamOptionsBuilder != null) {
            if (template.getDataStreamTemplate() == null) {
                throw new IllegalArgumentException(
                    "index template ["
                        + indexTemplateName
                        + "] specifies data stream options that can only be used in combination with a data stream"
                );
            }
            if (globalRetention != null) {
                // We cannot know for sure if the template will apply to internal data streams, so we use a simpler heuristic:
                // If all the index patterns start with a dot, we consider that all the connected data streams are internal.
                boolean isInternalDataStream = template.indexPatterns().stream().allMatch(indexPattern -> indexPattern.charAt(0) == '.');
                DataStreamOptions dataStreamOptions = dataStreamOptionsBuilder.build();
                if (dataStreamOptions.failureStore() != null && dataStreamOptions.failureStore().lifecycle() != null) {
                    dataStreamOptions.failureStore()
                        .lifecycle()
                        .addWarningHeaderIfDataRetentionNotEffective(globalRetention, isInternalDataStream);
                }
            }
        }
    }

    /**
     * Validate that by changing or adding {@code newTemplate}, there are
     * no unreferenced data streams. Note that this scenario is still possible
     * due to snapshot restores, but this validation is best-effort at template
     * addition/update time
     */
    public static void validateDataStreamsStillReferenced(
        ProjectMetadata candidateProject,
        ProjectMetadata previousProject,
        Set<String> updatedIndexTemplateNames
    ) {
        if (updatedIndexTemplateNames.isEmpty()) {
            // Nothing to validate
            return;
        }
        final Set<String> dataStreams = candidateProject.dataStreams()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().isSystem() == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        final Map<String, Set<String>> offendingTemplateToUnreferencedDataStreams = new HashMap<>();
        final Set<String> unreferencedDataStreamsWithoutTemplateChange = new HashSet<>();
        // For each data stream that we have, see whether it's covered by a different
        // template (which is great), or whether it's now uncovered by any template
        for (String dataStream : dataStreams) {
            final LinkedHashSet<String> originallyMatchingTemplates = findV2Templates(previousProject, dataStream, false);
            boolean unreferenced = false;
            if (originallyMatchingTemplates == null) {
                unreferenced = true;
            } else {
                // We found a template that still matches, great! Buuuuttt... check whether it
                // is a data stream template, as it's only useful if it has a data stream definition
                if (previousProject.templatesV2().get(originallyMatchingTemplates.getFirst()).getDataStreamTemplate() == null) {
                    unreferenced = true;
                }
            }
            if (unreferenced) {
                // The data stream was previously unreferenced, so any template changes are irrelevant to check
                continue;
            }
            String originalTemplate = originallyMatchingTemplates.getFirst();
            // Determine if the data stream is newly unreferenced after all template changes happen
            final LinkedHashSet<String> newlyMatchingTemplates = findV2Templates(candidateProject, dataStream, false);
            if (newlyMatchingTemplates == null) {
                // We have no template anymore after the changes. The only way this could happen is if the
                // previous template was removed or its patterns changed to not match anymore. Either way,
                // the previous template was changed and thus is the offender here
                if (updatedIndexTemplateNames.contains(originalTemplate)) {
                    offendingTemplateToUnreferencedDataStreams.computeIfAbsent(
                        originalTemplate,
                        (k) -> new HashSet<>()
                    ).add(dataStream);
                } else {
                    // Not sure how this could happen. Somehow we had a template in the last version that matched, but now we don't,
                    // and it wasn't one that changed. Assert on it and collect for graceful degradation.
                    unreferencedDataStreamsWithoutTemplateChange.add(dataStream);
                    assert false
                        : "data stream ["
                            + dataStream
                            + "] became unreferenced during template update. Previous template was ["
                            + originalTemplate
                            + "] but it was not a template that changed during the cluster state operation. Changed templates ["
                            + updatedIndexTemplateNames
                            + "]";
                }
            } else {
                // A template still matches, but again - check if it is a data stream template
                String newTemplate = newlyMatchingTemplates.getFirst();
                if (candidateProject.templatesV2().get(newTemplate).getDataStreamTemplate() == null) {
                    // Template is not a data stream template. Could be because
                    boolean newTemplateWasUpdated = updatedIndexTemplateNames.contains(newTemplate);
                    boolean oldTemplateWasUpdated = updatedIndexTemplateNames.contains(originalTemplate);
                    // Ok so this data stream is in an invalid state. Figure out which template change caused this.
                    if (newTemplateWasUpdated && oldTemplateWasUpdated) {
                        // Ok both templates changed, but we don't know if both changes are responsible for why the data stream broke.
                        // What causes a template to be the reason for breaking? Patterns, priority, and template changes.
                        // Does it matter? We're going to reject both operations. Just label both of them as the cause of the problem.
                        offendingTemplateToUnreferencedDataStreams.computeIfAbsent(
                            newTemplate,
                            (k) -> new HashSet<>()
                        ).add(dataStream);
                        offendingTemplateToUnreferencedDataStreams.computeIfAbsent(
                            originalTemplate,
                            (k) -> new HashSet<>()
                        ).add(dataStream);
                    } else if (newTemplateWasUpdated) {
                        // A new template was inserted ahead of the old one, and it is not a data stream template
                        // Just the new one is the problem
                        offendingTemplateToUnreferencedDataStreams.computeIfAbsent(
                            newTemplate,
                            (k) -> new HashSet<>()
                        ).add(dataStream);
                    } else {
                        // Old template was changed and the new template (unchanged) is now incorrectly applied
                        // The old one changing is the problem
                        offendingTemplateToUnreferencedDataStreams.computeIfAbsent(
                            originalTemplate,
                            (k) -> new HashSet<>()
                        ).add(dataStream);
                    }
                }
            }
        }

        // If we found any data streams that used to be covered, but will no longer be covered by
        // changing this template, then blow up with as much helpful information as we can muster
        if (offendingTemplateToUnreferencedDataStreams.isEmpty() == false) {
            IllegalArgumentException validationFailure = null;
            for (Map.Entry<String, Set<String>> templateErrors : offendingTemplateToUnreferencedDataStreams.entrySet()) {
                String templateName = templateErrors.getKey();
                Set<String> newlyUnreferenced = templateErrors.getValue();
                ComposableIndexTemplate newTemplate = candidateProject.templatesV2().get(templateName);
                IllegalArgumentException failure = new IllegalArgumentException(
                    "composable template ["
                        + templateName
                        + "] with index patterns "
                        + newTemplate.indexPatterns()
                        + ", priority ["
                        + newTemplate.priority()
                        + "] "
                        + (newTemplate.getDataStreamTemplate() == null ? "and no data stream configuration " : "")
                        + "would cause data streams "
                        + newlyUnreferenced
                        + " to no longer match a data stream template"
                );
                if (validationFailure == null) {
                    validationFailure = failure;
                } else {
                    validationFailure.addSuppressed(failure);
                }
            }
            throw validationFailure;
        } else if (unreferencedDataStreamsWithoutTemplateChange.isEmpty() == false) {
            // For sanity if assertions are disabled
            throw new IllegalArgumentException(
                "update for templates "
                    + updatedIndexTemplateNames
                    + "would cause data streams "
                    + unreferencedDataStreamsWithoutTemplateChange
                    + " to no longer match a data stream template"
            );
        }
    }

    /**
     * Validate that by changing or adding {@code newTemplate}, there are
     * no unreferenced data streams. Note that this scenario is still possible
     * due to snapshot restores, but this validation is best-effort at template
     * addition/update time
     */
    private static void validateDataStreamsStillReferencedOld(
        ProjectMetadata candidateProject,
        ProjectMetadata previousProject,
        String templateName,
        ComposableIndexTemplate newTemplate
    ) {
        final Set<String> dataStreams = candidateProject.dataStreams()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().isSystem() == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

        BiFunction<ProjectMetadata, Map<String, ComposableIndexTemplate>, Set<String>> findUnreferencedDataStreams =
            (meta, composableTemplates) -> {
            final Set<String> unreferenced = new HashSet<>();
            // For each data stream that we have, see whether it's covered by a different
            // template (which is great), or whether it's now uncovered by any template
            for (String dataStream : dataStreams) {
                var v2Templates = findV2Templates(meta, composableTemplates.entrySet(), dataStream, false, false);
                if (v2Templates.isEmpty()) {
                    unreferenced.add(dataStream);
                } else {
                    final String matchingTemplate = v2Templates.getFirst().v1();
                    // We found a template that still matches, great! Buuuuttt... check whether it
                    // is a data stream template, as it's only useful if it has a data stream definition
                    if (composableTemplates.get(matchingTemplate).getDataStreamTemplate() == null) {
                        unreferenced.add(dataStream);
                    }
                }
            }
            return unreferenced;
        };

        // Find data streams that are currently unreferenced
        final Set<String> currentlyUnreferenced = findUnreferencedDataStreams.apply(previousProject, previousProject.templatesV2());

        // Generate a map as if the new template were actually in the cluster state
        final var updatedTemplatesMap = new HashMap<>(candidateProject.templatesV2());
        updatedTemplatesMap.put(templateName, newTemplate);
        // Find the data streams that would be unreferenced now that the template is updated/added
        final Set<String> newlyUnreferenced = findUnreferencedDataStreams.apply(candidateProject, updatedTemplatesMap);

        // If we found any data streams that used to be covered, but will no longer be covered by
        // changing this template, then blow up with as much helpful information as we can muster
        if (newlyUnreferenced.size() > currentlyUnreferenced.size()) {
            throw new IllegalArgumentException(
                "composable template ["
                    + templateName
                    + "] with index patterns "
                    + newTemplate.indexPatterns()
                    + ", priority ["
                    + newTemplate.priority()
                    + "] "
                    + (newTemplate.getDataStreamTemplate() == null ? "and no data stream configuration " : "")
                    + "would cause data streams "
                    + newlyUnreferenced
                    + " to no longer match a data stream template"
            );
        }
    }

    /**
     * Return a map of v1 template names to their index patterns for v1 templates that would overlap
     * with the given v2 template's index patterns.
     */
    public static Map<String, List<String>> findConflictingV1Templates(
        final ProjectMetadata project,
        final String candidateName,
        final List<String> indexPatterns
    ) {
        // No need to determinize the automaton, as it is only used to check for intersection with another automaton.
        // Determinization is avoided because it can fail or become very costly due to state explosion.
        Automaton v2automaton = Regex.simpleMatchToNonDeterminizedAutomaton(indexPatterns.toArray(Strings.EMPTY_ARRAY));
        Map<String, List<String>> overlappingTemplates = new HashMap<>();
        for (Map.Entry<String, IndexTemplateMetadata> cursor : project.templates().entrySet()) {
            String name = cursor.getKey();
            IndexTemplateMetadata template = cursor.getValue();
            // No need to determinize the automaton, as it is only used to check for intersection with another automaton.
            Automaton v1automaton = Regex.simpleMatchToNonDeterminizedAutomaton(template.patterns().toArray(Strings.EMPTY_ARRAY));
            if (Operations.isEmpty(Operations.intersection(v2automaton, v1automaton)) == false) {
                logger.debug(
                    "composable template {} and legacy template {} would overlap: {} <=> {}",
                    candidateName,
                    name,
                    indexPatterns,
                    template.patterns()
                );
                overlappingTemplates.put(name, template.patterns());
            }
        }
        return overlappingTemplates;
    }

    /**
     * Return a map of v2 template names to their index patterns for v2 templates that would overlap
     * with the given template's index patterns.
     */
    public static Map<String, List<String>> findConflictingV2Templates(
        final ProjectMetadata project,
        final String candidateName,
        final List<String> indexPatterns
    ) {
        return findConflictingV2Templates(project.templatesV2(), candidateName, indexPatterns, false, 0L);
    }

    /**
     * Return a map of v2 template names to their index patterns for v2 templates that would overlap
     * with the given template's index patterns.
     *
     * Based on the provided checkPriority and priority parameters this aims to report the overlapping
     * index templates regardless of the priority (ie. checkPriority == false) or otherwise overlapping
     * templates with the same priority as the given priority parameter (this is useful when trying to
     * add a new template, as we don't support multiple overlapping, from an index pattern perspective,
     * index templates with the same priority).
     */
    static Map<String, List<String>> findConflictingV2Templates(
        final Map<String, ComposableIndexTemplate> templatesV2,
        final String candidateName,
        final List<String> indexPatterns,
        boolean checkPriority,
        long priority
    ) {
        // No need to determinize the automaton, as it is only used to check for intersection with another automaton.
        // Determinization is avoided because it can fail or become very costly due to state explosion.
        Automaton v1automaton = Regex.simpleMatchToNonDeterminizedAutomaton(indexPatterns.toArray(Strings.EMPTY_ARRAY));
        Map<String, List<String>> overlappingTemplates = new TreeMap<>();
        for (Map.Entry<String, ComposableIndexTemplate> entry : templatesV2.entrySet()) {
            String name = entry.getKey();
            ComposableIndexTemplate template = entry.getValue();
            // No need to determinize the automaton, as it is only used to check for intersection with another automaton.
            // Determinization is avoided because it can fail or become very costly due to state explosion.
            Automaton v2automaton = Regex.simpleMatchToNonDeterminizedAutomaton(template.indexPatterns().toArray(Strings.EMPTY_ARRAY));
            if (Operations.isEmpty(Operations.intersection(v1automaton, v2automaton)) == false) {
                if (checkPriority == false || priority == template.priorityOrZero()) {
                    logger.debug(
                        "legacy template {} and composable template {} would overlap: {} <=> {}",
                        candidateName,
                        name,
                        indexPatterns,
                        template.indexPatterns()
                    );
                    overlappingTemplates.put(name, template.indexPatterns());
                }
            }
        }
        // if the candidate was a V2 template that already exists in the cluster state it will "overlap" with itself so remove it from the
        // results
        overlappingTemplates.remove(candidateName);
        return overlappingTemplates;
    }

    /**
     * Remove the given index template from the cluster state. The index template name
     * supports simple regex wildcards for removing multiple index templates at a time.
     */
    public void removeIndexTemplateV2(
        final ProjectId projectId,
        final String[] names,
        final TimeValue masterTimeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask(
            "remove-index-template-v2 [" + String.join(",", names) + "]",
            new TemplateClusterStateUpdateTask(listener, projectId) {
                @Override
                public ProjectMetadata execute(ProjectMetadata currentProject) {
                    return innerRemoveIndexTemplateV2(currentProject, names);
                }
            },
            masterTimeout
        );
    }

    // Public because it's used by ReservedComposableIndexTemplateAction
    public static ProjectMetadata innerRemoveIndexTemplateV2(ProjectMetadata project, String... names) {
        Set<String> templateNames = new HashSet<>();

        if (names.length > 1) {
            Set<String> missingNames = null;
            for (String name : names) {
                if (project.templatesV2().containsKey(name)) {
                    templateNames.add(name);
                } else {
                    // wildcards are not supported, so if a name with a wildcard is specified then
                    // the else clause gets executed, because template names can't contain a wildcard.
                    if (missingNames == null) {
                        missingNames = new LinkedHashSet<>();
                    }
                    missingNames.add(name);
                }
            }

            if (missingNames != null) {
                throw new IndexTemplateMissingException(String.join(",", missingNames));
            }
        } else {
            final String name = names[0];
            for (String templateName : project.templatesV2().keySet()) {
                if (Regex.simpleMatch(name, templateName)) {
                    templateNames.add(templateName);
                }
            }
            if (templateNames.isEmpty()) {
                // if it's a match all pattern, and no templates are found (we have none), don't
                // fail with index missing...
                boolean isMatchAll = false;
                if (Regex.isMatchAllPattern(name)) {
                    isMatchAll = true;
                }
                if (isMatchAll) {
                    return project;
                } else {
                    throw new IndexTemplateMissingException(name);
                }
            }
        }

        Set<String> dataStreamsUsingTemplates = dataStreamsExclusivelyUsingTemplates(project, templateNames);
        if (dataStreamsUsingTemplates.size() > 0) {
            throw new IllegalArgumentException(
                "unable to remove composable templates "
                    + new TreeSet<>(templateNames)
                    + " as they are in use by a data streams "
                    + new TreeSet<>(dataStreamsUsingTemplates)
            );
        }

        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);
        for (String templateName : templateNames) {
            logger.info("removing index template [{}]", templateName);
            builder.removeIndexTemplate(templateName);
        }
        return builder.build();
    }

    /**
     * Returns the data stream names that solely match the patterns of the template names that were provided and no
     * other templates. This means that the returned data streams depend on these templates which has implications for
     * these templates, for example they cannot be removed.
     */
    static Set<String> dataStreamsExclusivelyUsingTemplates(final ProjectMetadata projectMetadata, final Set<String> templateNames) {
        Set<String> namePatterns = templateNames.stream()
            .map(templateName -> projectMetadata.templatesV2().get(templateName))
            .filter(Objects::nonNull)
            .map(ComposableIndexTemplate::indexPatterns)
            .flatMap(List::stream)
            .collect(Collectors.toSet());

        return projectMetadata.dataStreams()
            .values()
            .stream()
            // Limit to checking data streams that match any of the templates' index patterns
            .filter(ds -> namePatterns.stream().anyMatch(pattern -> Regex.simpleMatch(pattern, ds.getName())))
            .filter(ds -> {
                // Retrieve the templates that match the data stream name ordered by priority
                List<Tuple<String, ComposableIndexTemplate>> candidates = findV2CandidateTemplates(
                    projectMetadata.templatesV2().entrySet(),
                    ds.getName(),
                    ds.isHidden(),
                    false
                );
                if (candidates.isEmpty()) {
                    throw new IllegalStateException("Data stream " + ds.getName() + " did not match any composable index templates.");
                }

                // Limit data streams that can ONLY use any of the specified templates, we do this by filtering
                // the matching templates that are others than the ones requested and could be a valid template to use.
                return candidates.stream()
                    .noneMatch(
                        template -> templateNames.contains(template.v1()) == false
                            && isGlobalAndHasIndexHiddenSetting(template.v2(), projectMetadata.componentTemplates()) == false
                    );
            })
            .map(DataStream::getName)
            .collect(Collectors.toSet());
    }

    public void putTemplate(
        final ProjectId projectId,
        final PutRequest request,
        final TimeValue timeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        updatedSettingsBuilder.put(request.settings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        request.settings(updatedSettingsBuilder.build());

        if (request.name == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a name"));
            return;
        }
        if (request.indexPatterns == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a template"));
            return;
        }

        try {
            validate(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        final IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder(request.name);

        taskQueue.submitTask(
            "create-index-template [" + request.name + "], cause [" + request.cause + "]",
            new TemplateClusterStateUpdateTask(listener, projectId) {
                @Override
                public ProjectMetadata execute(ProjectMetadata project) throws Exception {
                    validateTemplate(request.settings, request.mappings, indicesService);
                    return innerPutTemplate(project, request, templateBuilder);
                }
            },
            timeout
        );
    }

    // Package visible for testing
    static ProjectMetadata innerPutTemplate(
        ProjectMetadata currentState,
        PutRequest request,
        IndexTemplateMetadata.Builder templateBuilder
    ) {
        // Flag for whether this is updating an existing template or adding a new one
        // TODO: in 8.0+, only allow updating index templates, not adding new ones
        boolean isUpdate = currentState.templates().containsKey(request.name);
        if (request.create && isUpdate) {
            throw new IllegalArgumentException("index_template [" + request.name + "] already exists");
        }
        boolean isUpdateAndPatternsAreUnchanged = isUpdate
            && currentState.templates().get(request.name).patterns().equals(request.indexPatterns);

        Map<String, List<String>> overlaps = findConflictingV2Templates(currentState, request.name, request.indexPatterns);
        if (overlaps.size() > 0) {
            // Be less strict (just a warning) if we're updating an existing template or this is a match-all template
            if (isUpdateAndPatternsAreUnchanged || request.indexPatterns.stream().anyMatch(Regex::isMatchAllPattern)) {
                String warning = String.format(
                    Locale.ROOT,
                    "legacy template [%s] has index patterns %s matching patterns"
                        + " from existing composable templates [%s] with patterns (%s); this template [%s] may be ignored in favor"
                        + " of a composable template at index creation time",
                    request.name,
                    request.indexPatterns,
                    Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                    overlaps.entrySet().stream().map(e -> e.getKey() + " => " + e.getValue()).collect(Collectors.joining(",")),
                    request.name
                );
                logger.warn(warning);
                HeaderWarning.addWarning(warning);
            } else {
                // Otherwise, this is a hard error, the user should use V2 index templates instead
                String error = String.format(
                    Locale.ROOT,
                    "legacy template [%s] has index patterns %s matching patterns"
                        + " from existing composable templates [%s] with patterns (%s), use composable templates"
                        + " (/_index_template) instead",
                    request.name,
                    request.indexPatterns,
                    Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                    overlaps.entrySet().stream().map(e -> e.getKey() + " => " + e.getValue()).collect(Collectors.joining(","))
                );
                logger.error(error);
                throw new IllegalArgumentException(error);
            }
        }

        templateBuilder.order(request.order);
        templateBuilder.version(request.version);
        templateBuilder.patterns(request.indexPatterns);
        templateBuilder.settings(request.settings);

        if (request.mappings != null) {
            try {
                templateBuilder.putMapping(MapperService.SINGLE_MAPPING_NAME, request.mappings);
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping: {}", e, request.mappings);
            }
        }

        for (Alias alias : request.aliases) {
            AliasMetadata aliasMetadata = AliasMetadata.builder(alias.name())
                .filter(alias.filter())
                .indexRouting(alias.indexRouting())
                .searchRouting(alias.searchRouting())
                .writeIndex(alias.writeIndex())
                .isHidden(alias.isHidden())
                .build();
            templateBuilder.putAlias(aliasMetadata);
        }
        IndexTemplateMetadata template = templateBuilder.build();
        IndexTemplateMetadata existingTemplate = currentState.templates().get(request.name);
        if (template.equals(existingTemplate)) {
            // The template is unchanged, therefore there is no need for a cluster state update
            return currentState;
        }

        logger.info("adding template [{}] for index patterns {}", request.name, request.indexPatterns);
        return ProjectMetadata.builder(currentState).put(template).build();
    }

    /**
     * A private, local alternative to elements.stream().anyMatch(predicate) for micro-optimization reasons.
     */
    private static <T> boolean anyMatch(final List<T> elements, final Predicate<T> predicate) {
        for (T e : elements) {
            if (predicate.test(e)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A private, local alternative to elements.stream().noneMatch(predicate) for micro-optimization reasons.
     */
    private static <T> boolean noneMatch(final List<T> elements, final Predicate<T> predicate) {
        for (T e : elements) {
            if (predicate.test(e)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A private, local alternative to elements.stream().filter(predicate).findFirst() for micro-optimization reasons.
     */
    private static <T> Optional<T> findFirst(final List<T> elements, final Predicate<T> predicate) {
        for (T e : elements) {
            if (predicate.test(e)) {
                return Optional.of(e);
            }
        }
        return Optional.empty();
    }

    /**
     * Finds index templates whose index pattern matched with the given index name. In the case of
     * hidden indices, a template with a match all pattern or global template will not be returned.
     *
     * @param projectMetadata The {@link ProjectMetadata} containing all of the {@link IndexTemplateMetadata} values
     * @param indexName The name of the index that templates are being found for
     * @param isHidden Whether or not the index is known to be hidden. May be {@code null} if the index
     *                 being hidden has not been explicitly requested. When {@code null} if the result
     *                 of template application results in a hidden index, then global templates will
     *                 not be returned
     * @return a list of templates sorted by {@link IndexTemplateMetadata#order()} descending.
     *
     */
    public static List<IndexTemplateMetadata> findV1Templates(
        ProjectMetadata projectMetadata,
        String indexName,
        @Nullable Boolean isHidden
    ) {
        final String resolvedIndexName = IndexNameExpressionResolver.DateMathExpressionResolver.resolveExpression(indexName);
        final Predicate<String> patternMatchPredicate = pattern -> Regex.simpleMatch(pattern, resolvedIndexName);
        final List<IndexTemplateMetadata> matchedTemplates = new ArrayList<>();
        for (IndexTemplateMetadata template : projectMetadata.templates().values()) {
            if (isHidden == null || isHidden == Boolean.FALSE) {
                if (anyMatch(template.patterns(), patternMatchPredicate)) {
                    matchedTemplates.add(template);
                }
            } else {
                assert isHidden == Boolean.TRUE;
                final boolean isNotMatchAllTemplate = noneMatch(template.patterns(), Regex::isMatchAllPattern);
                if (isNotMatchAllTemplate) {
                    if (anyMatch(template.patterns(), patternMatchPredicate)) {
                        matchedTemplates.add(template);
                    }
                }
            }
        }
        CollectionUtil.timSort(matchedTemplates, Comparator.comparingInt(IndexTemplateMetadata::order).reversed());

        // this is complex but if the index is not hidden in the create request but is hidden as the result of template application,
        // then we need to exclude global templates
        if (isHidden == null) {
            final Optional<IndexTemplateMetadata> templateWithHiddenSetting = findFirst(
                matchedTemplates,
                template -> IndexMetadata.INDEX_HIDDEN_SETTING.exists(template.settings())
            );
            if (templateWithHiddenSetting.isPresent()) {
                final boolean templatedIsHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(templateWithHiddenSetting.get().settings());
                if (templatedIsHidden) {
                    // remove the global templates
                    matchedTemplates.removeIf(current -> anyMatch(current.patterns(), Regex::isMatchAllPattern));
                }
                // validate that hidden didn't change
                final Optional<IndexTemplateMetadata> templateWithHiddenSettingPostRemoval = findFirst(
                    matchedTemplates,
                    template -> IndexMetadata.INDEX_HIDDEN_SETTING.exists(template.settings())
                );
                if (templateWithHiddenSettingPostRemoval.isEmpty()
                    || templateWithHiddenSetting.get() != templateWithHiddenSettingPostRemoval.get()) {
                    throw new IllegalStateException(
                        "A global index template ["
                            + templateWithHiddenSetting.get().name()
                            + "] defined the index hidden setting, which is not allowed"
                    );
                }
            }
        }
        return Collections.unmodifiableList(matchedTemplates);
    }

    /**
     * Return the name (id) of the highest matching index template for the given index name. In
     * the event that no templates are matched, {@code null} is returned.
     */
    @Nullable
    public static String findV2Template(ProjectMetadata projectMetadata, String indexName, boolean isHidden) {
        List<Tuple<String, ComposableIndexTemplate>> candidates = findV2Templates(
            projectMetadata,
            projectMetadata.templatesV2().entrySet(),
            indexName,
            isHidden,
            false
        );
        if (candidates.isEmpty()) {
            return null;
        } else {
            return candidates.get(0).v1();
        }
    }

    /**
     * Return the name (id) of the highest matching index template out of the provided templates (that <i>need</i> to be sorted descending
     * on priority beforehand), or the given index name. In the event that no templates are matched, {@code null} is returned.
     */
    @Nullable
    public static String findV2TemplateFromSortedList(
        ProjectMetadata projectMetadata,
        Collection<Map.Entry<String, ComposableIndexTemplate>> templates,
        String indexName,
        boolean isHidden
    ) {
        List<Tuple<String, ComposableIndexTemplate>> candidates = findV2Templates(projectMetadata, templates, indexName, isHidden, true);
        if (candidates.isEmpty()) {
            return null;
        } else {
            return candidates.get(0).v1();
        }
    }

    /**
     * Return the names (id) of all matching index templates for the given index name. In
     * the event that no templates are matched, {@code null} is returned. This includes
     * the same validation logic as {@link MetadataIndexTemplateService#findV2Template}
     * for the lead template
     */
    @Nullable
    public static LinkedHashSet<String> findV2Templates(ProjectMetadata projectMetadata, String indexName, boolean isHidden) {
        List<Tuple<String, ComposableIndexTemplate>> candidates = findV2Templates(
            projectMetadata,
            projectMetadata.templatesV2().entrySet(),
            indexName,
            isHidden,
            false
        );
        LinkedHashSet<String> resultNames = LinkedHashSet.newLinkedHashSet(candidates.size());
        for (Tuple<String, ComposableIndexTemplate> candidate : candidates) {
            resultNames.add(candidate.v1());
        }
        return resultNames;
    }

    /**
     * Return the name (id) and definition of the highest matching index template, out of the provided templates, for the given index
     * name. In the event that no templates are matched, {@code an empty list} is returned.
     */
    private static List<Tuple<String, ComposableIndexTemplate>> findV2Templates(
        ProjectMetadata projectMetadata,
        Collection<Map.Entry<String, ComposableIndexTemplate>> templates,
        String indexName,
        boolean isHidden,
        boolean exitOnFirstMatch
    ) {
        final List<Tuple<String, ComposableIndexTemplate>> candidates = findV2CandidateTemplates(
            templates,
            indexName,
            isHidden,
            exitOnFirstMatch
        );
        if (candidates.isEmpty()) {
            return candidates;
        }

        ComposableIndexTemplate winner = candidates.get(0).v2();
        String winnerName = candidates.get(0).v1();

        // if the "winner" template is a global template that specifies the `index.hidden` setting (which is not allowed, so it'd be due to
        // a restored index cluster state that modified a component template used by this global template such that it has this setting)
        // we will fail and the user will have to update the index template and remove this setting or update the corresponding component
        // template that contributes to the index template resolved settings
        if (isGlobalAndHasIndexHiddenSetting(winner, projectMetadata.componentTemplates())) {
            throw new IllegalStateException(
                "global index template ["
                    + winnerName
                    + "], composed of component templates ["
                    + String.join(",", winner.composedOf())
                    + "] defined the index.hidden setting, which is not allowed"
            );
        }

        return candidates;
    }

    /**
     * Return an ordered list of the name (id) and composable index templates that would apply to an index. The first
     * one is the winner template that is applied to this index. In the event that no templates are matched,
     * an empty list is returned.
     * @param templates a list of template entries (name, template) - needs to be sorted when {@code exitOnFirstMatch} is {@code true}
     * @param indexName the index (or data stream) name that should be used for matching the index patterns on the templates
     * @param isHidden whether {@code indexName} belongs to a hidden index - this option is redundant for data streams, as backing indices
     *                 of data streams will always be returned, regardless of whether the data stream is hidden or not
     * @param exitOnFirstMatch if true, we return immediately after finding a match. That means that the <code>templates</code>
     *                         parameter needs to be sorted based on priority (descending) for this method to return a sensible result,
     *                         otherwise this method would just return the first template that matches the name, in an unspecified order
     */
    private static List<Tuple<String, ComposableIndexTemplate>> findV2CandidateTemplates(
        Collection<Map.Entry<String, ComposableIndexTemplate>> templates,
        String indexName,
        boolean isHidden,
        boolean exitOnFirstMatch
    ) {
        assert exitOnFirstMatch == false || areTemplatesSorted(templates) : "Expected templates to be sorted";
        final String resolvedIndexName = IndexNameExpressionResolver.DateMathExpressionResolver.resolveExpression(indexName);
        final Predicate<String> patternMatchPredicate = pattern -> Regex.simpleMatch(pattern, resolvedIndexName);
        final List<Tuple<String, ComposableIndexTemplate>> candidates = new ArrayList<>();
        for (Map.Entry<String, ComposableIndexTemplate> entry : templates) {
            final String name = entry.getKey();
            final ComposableIndexTemplate template = entry.getValue();
            /*
             * We do not ordinarily return match-all templates for hidden indices. But all backing indices for data streams are hidden,
             * and we do want to return even match-all templates for those. Not doing so can result in a situation where a data stream is
             * built with a template that none of its indices match.
             */
            if (anyMatch(template.indexPatterns(), Regex::isMatchAllPattern) && isHidden && template.getDataStreamTemplate() == null) {
                continue;
            }
            if (anyMatch(template.indexPatterns(), patternMatchPredicate)) {
                candidates.add(Tuple.tuple(name, template));
                if (exitOnFirstMatch) {
                    return candidates;
                }
            }
        }

        CollectionUtil.timSort(candidates, Comparator.comparing(candidate -> candidate.v2().priorityOrZero(), Comparator.reverseOrder()));
        return candidates;
    }

    private static boolean areTemplatesSorted(Collection<Map.Entry<String, ComposableIndexTemplate>> templates) {
        ComposableIndexTemplate previousTemplate = null;
        for (Map.Entry<String, ComposableIndexTemplate> template : templates) {
            if (previousTemplate != null && template.getValue().priorityOrZero() > previousTemplate.priorityOrZero()) {
                return false;
            }
            previousTemplate = template.getValue();
        }
        return true;
    }

    // Checks if a global template specifies the `index.hidden` setting. This check is important because a global
    // template shouldn't specify the `index.hidden` setting, we leave it up to the caller to handle this situation.
    private static boolean isGlobalAndHasIndexHiddenSetting(
        ComposableIndexTemplate template,
        Map<String, ComponentTemplate> componentTemplates
    ) {
        return anyMatch(template.indexPatterns(), Regex::isMatchAllPattern)
            && IndexMetadata.INDEX_HIDDEN_SETTING.exists(resolveSettings(template, componentTemplates));
    }

    /**
     * Collect the given v2 template into an ordered list of mappings.
     */
    public static List<CompressedXContent> collectMappings(
        final ProjectMetadata projectMetadata,
        final String templateName,
        final String indexName
    ) {
        final ComposableIndexTemplate template = projectMetadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve mappings for a template [" + templateName + "] that did not exist in the cluster state";
        if (template == null) {
            return List.of();
        }

        final Map<String, ComponentTemplate> componentTemplates = projectMetadata.componentTemplates();
        return collectMappings(template, componentTemplates, indexName);
    }

    /**
     * Collect the given v2 template into an ordered list of mappings.
     */
    public static List<CompressedXContent> collectMappings(
        final ComposableIndexTemplate template,
        final Map<String, ComponentTemplate> componentTemplates,
        final String indexName
    ) {
        Objects.requireNonNull(template, "Composable index template must be provided");
        // Check if this is a failure store index, and if it is, discard any template mappings. Failure store mappings are predefined.
        if (template.getDataStreamTemplate() != null && indexName.startsWith(DataStream.FAILURE_STORE_PREFIX)) {
            return List.of(
                DataStreamFailureStoreDefinition.DATA_STREAM_FAILURE_STORE_MAPPING,
                ComposableIndexTemplate.DataStreamTemplate.DATA_STREAM_MAPPING_SNIPPET
            );
        }
        List<CompressedXContent> mappings = template.composedOf()
            .stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::mappings)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(LinkedList::new));
        // Add the actual index template's mappings, since it takes the highest precedence
        Optional.ofNullable(template.template()).map(Template::mappings).ifPresent(mappings::add);
        if (template.getDataStreamTemplate() != null && isDataStreamIndex(indexName)) {
            // add a default mapping for the `@timestamp` field, at the lowest precedence, to make bootstrapping data streams more
            // straightforward as all backing indices are required to have a timestamp field
            if (template.getDataStreamTemplate().isAllowCustomRouting()) {
                mappings.add(0, DEFAULT_TIMESTAMP_MAPPING_WITH_ROUTING);
            } else {
                mappings.add(0, DEFAULT_TIMESTAMP_MAPPING_WITHOUT_ROUTING);
            }
        }

        // Only include _timestamp mapping snippet if creating backing index.
        if (isDataStreamIndex(indexName)) {
            // Only if template has data stream definition this should be added and
            // adding this template last, since _timestamp field should have highest precedence:
            if (template.getDataStreamTemplate() != null) {
                mappings.add(ComposableIndexTemplate.DataStreamTemplate.DATA_STREAM_MAPPING_SNIPPET);
            }
        }
        return Collections.unmodifiableList(mappings);
    }

    private static boolean isDataStreamIndex(String indexName) {
        return indexName.startsWith(DataStream.BACKING_INDEX_PREFIX) || indexName.startsWith(DataStream.FAILURE_STORE_PREFIX);
    }

    /**
     * Resolve index settings for the given list of v1 templates, templates are apply in reverse
     * order since they should be provided in order of priority/order
     */
    public static Settings resolveSettings(final List<IndexTemplateMetadata> templates) {
        Settings.Builder templateSettings = Settings.builder();
        // apply templates, here, in reverse order, since first ones are better matching
        for (int i = templates.size() - 1; i >= 0; i--) {
            templateSettings.put(templates.get(i).settings());
        }
        return templateSettings.build();
    }

    /**
     * Resolve the given v2 template into a collected {@link Settings} object
     */
    public static Settings resolveSettings(final ProjectMetadata projectMetadata, final String templateName) {
        final ComposableIndexTemplate template = projectMetadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve settings for a template [" + templateName + "] that did not exist in the cluster state";
        if (template == null) {
            return Settings.EMPTY;
        }
        return resolveSettings(template, projectMetadata.componentTemplates());
    }

    /**
     * Resolve the provided v2 template and component templates into a collected {@link Settings} object
     */
    public static Settings resolveSettings(ComposableIndexTemplate template, Map<String, ComponentTemplate> componentTemplates) {
        Objects.requireNonNull(template, "attempted to resolve settings for a null template");
        Objects.requireNonNull(componentTemplates, "attempted to resolve settings with null component templates");
        List<Settings> componentSettings = template.composedOf()
            .stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::settings)
            .filter(Objects::nonNull)
            .toList();

        Settings.Builder templateSettings = Settings.builder();
        componentSettings.forEach(templateSettings::put);
        // Add the actual index template's settings to the end, since it takes the highest precedence.
        Optional.ofNullable(template.template()).map(Template::settings).ifPresent(templateSettings::put);
        return templateSettings.build();
    }

    /**
     * Resolve the given v1 templates into an ordered list of aliases
     */
    public static List<Map<String, AliasMetadata>> resolveAliases(final List<IndexTemplateMetadata> templates) {
        final List<Map<String, AliasMetadata>> resolvedAliases = new ArrayList<>();
        templates.forEach(template -> {
            if (template.aliases() != null) {
                Map<String, AliasMetadata> aliasMeta = new HashMap<>();
                for (Map.Entry<String, AliasMetadata> cursor : template.aliases().entrySet()) {
                    aliasMeta.put(cursor.getKey(), cursor.getValue());
                }
                resolvedAliases.add(aliasMeta);
            }
        });
        return Collections.unmodifiableList(resolvedAliases);
    }

    /**
     * Resolve the given v2 template name into an ordered list of aliases
     */
    public static List<Map<String, AliasMetadata>> resolveAliases(final ProjectMetadata projectMetadata, final String templateName) {
        final ComposableIndexTemplate template = projectMetadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve aliases for a template [" + templateName + "] that did not exist in the cluster state";
        return resolveAliases(projectMetadata, template);
    }

    /**
     * Resolve the given v2 template into an ordered list of aliases
     */
    static List<Map<String, AliasMetadata>> resolveAliases(final ProjectMetadata projectMetadata, final ComposableIndexTemplate template) {
        if (template == null) {
            return List.of();
        }
        final Map<String, ComponentTemplate> componentTemplates = projectMetadata.componentTemplates();
        return resolveAliases(template, componentTemplates);
    }

    /**
     * Resolve the given v2 template and component templates into an ordered list of aliases
     */
    static List<Map<String, AliasMetadata>> resolveAliases(
        final ComposableIndexTemplate template,
        final Map<String, ComponentTemplate> componentTemplates
    ) {
        Objects.requireNonNull(template, "attempted to resolve aliases for a null template");
        Objects.requireNonNull(componentTemplates, "attempted to resolve aliases with null component templates");
        List<Map<String, AliasMetadata>> aliases = template.composedOf()
            .stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::aliases)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(ArrayList::new));

        // Add the actual index template's aliases to the end if they exist
        Optional.ofNullable(template.template()).map(Template::aliases).ifPresent(aliases::add);

        // Aliases are applied in order, but subsequent alias configuration from the same name is
        // ignored, so in order for the order to be correct, alias configuration should be in order
        // of precedence (with the index template first)
        Collections.reverse(aliases);
        return Collections.unmodifiableList(aliases);
    }

    /**
     * Resolve the given v2 template into a {@link DataStreamLifecycle} object
     */
    @Nullable
    public static DataStreamLifecycle.Builder resolveLifecycle(ProjectMetadata metadata, final String templateName) {
        final ComposableIndexTemplate template = metadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve lifecycle for a template [" + templateName + "] that did not exist in the cluster state";
        if (template == null) {
            return null;
        }
        return resolveLifecycle(template, metadata.componentTemplates());
    }

    /**
     * Resolve the provided v2 template and component templates into a {@link DataStreamLifecycle} object
     */
    @Nullable
    public static DataStreamLifecycle.Builder resolveLifecycle(
        ComposableIndexTemplate template,
        Map<String, ComponentTemplate> componentTemplates
    ) {
        Objects.requireNonNull(template, "attempted to resolve lifecycle for a null template");
        Objects.requireNonNull(componentTemplates, "attempted to resolve lifecycle with null component templates");

        List<DataStreamLifecycle.Template> lifecycles = new ArrayList<>();
        for (String componentTemplateName : template.composedOf()) {
            if (componentTemplates.containsKey(componentTemplateName) == false) {
                continue;
            }
            DataStreamLifecycle.Template lifecycle = componentTemplates.get(componentTemplateName).template().lifecycle();
            if (lifecycle != null) {
                lifecycles.add(lifecycle);
            }
        }
        // The actual index template's lifecycle has the highest precedence.
        if (template.template() != null && template.template().lifecycle() != null) {
            lifecycles.add(template.template().lifecycle());
        }
        return composeDataLifecycles(lifecycles);
    }

    /**
     * This method composes a series of lifecycles to a final one. The lifecycles are getting composed one level deep,
     * meaning that the keys present on the latest lifecycle will override the ones of the others. If a key is missing
     * then it keeps the value of the previous lifecycles. For example, if we have the following two lifecycles:
     * [
     *   {
     *     "lifecycle": {
     *       "enabled": true,
     *       "data_retention" : "10d"
     *     }
     *   },
     *   {
     *     "lifecycle": {
     *       "enabled": true,
     *       "data_retention" : "20d"
     *     }
     *   }
     * ]
     * The result will be { "lifecycle": { "enabled": true, "data_retention" : "20d"}} because the second data retention overrides the
     * first. However, if we have the following two lifecycles:
     * [
     *   {
     *     "lifecycle": {
     *       "enabled": false,
     *       "data_retention" : "10d"
     *     }
     *   },
     *   {
     *   "lifecycle": {
     *      "enabled": true
     *   }
     *   }
     * ]
     * The result will be { "lifecycle": { "enabled": true, "data_retention" : "10d"} } because the latest lifecycle does not have any
     * information on retention.
     * @param lifecycles a sorted list of lifecycles in the order that they will be composed
     * @return the builder that will build the final lifecycle or the template
     */
    @Nullable
    public static DataStreamLifecycle.Builder composeDataLifecycles(List<DataStreamLifecycle.Template> lifecycles) {
        DataStreamLifecycle.Builder builder = null;
        for (DataStreamLifecycle.Template current : lifecycles) {
            if (builder == null) {
                builder = DataStreamLifecycle.builder(current);
            } else {
                builder.composeTemplate(current);
            }
        }
        return builder;
    }

    /**
     * Resolve the given v2 template into a {@link DataStreamOptions.Builder} object that can be built to either a
     * {@link DataStreamOptions} or the equivalent {@link DataStreamOptions.Template}.
     */
    @Nullable
    public static DataStreamOptions.Builder resolveDataStreamOptions(final ProjectMetadata projectMetadata, final String templateName) {
        final ComposableIndexTemplate template = projectMetadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve data stream options for a template [" + templateName + "] that did not exist in the cluster state";
        if (template == null) {
            return null;
        }
        return resolveDataStreamOptions(template, projectMetadata.componentTemplates());
    }

    /**
     * Resolve the provided v2 template and component templates into a {@link DataStreamOptions.Builder} object that can be built to
     * either a {@link DataStreamOptions} or the equivalent {@link DataStreamOptions.Template}.
     */
    @Nullable
    public static DataStreamOptions.Builder resolveDataStreamOptions(
        ComposableIndexTemplate template,
        Map<String, ComponentTemplate> componentTemplates
    ) {
        Objects.requireNonNull(template, "attempted to resolve data stream for a null template");
        Objects.requireNonNull(componentTemplates, "attempted to resolve data stream options with null component templates");

        List<ResettableValue<DataStreamOptions.Template>> dataStreamOptionsList = new ArrayList<>();
        for (String componentTemplateName : template.composedOf()) {
            if (componentTemplates.containsKey(componentTemplateName) == false) {
                continue;
            }
            ResettableValue<DataStreamOptions.Template> dataStreamOptions = componentTemplates.get(componentTemplateName)
                .template()
                .resettableDataStreamOptions();
            if (dataStreamOptions.isDefined()) {
                dataStreamOptionsList.add(dataStreamOptions);
            }
        }
        // The actual index template's data stream options have the highest precedence.
        if (template.template() != null && template.template().resettableDataStreamOptions().isDefined()) {
            dataStreamOptionsList.add(template.template().resettableDataStreamOptions());
        }
        return composeDataStreamOptions(dataStreamOptionsList);
    }

    /**
     * This method composes a series of data streams options to a final one.
     * @param dataStreamOptionsList a sorted list of data stream options in the order that they will be composed
     * @return the final data stream option configuration
     */
    @Nullable
    public static DataStreamOptions.Builder composeDataStreamOptions(
        List<ResettableValue<DataStreamOptions.Template>> dataStreamOptionsList
    ) {
        if (dataStreamOptionsList.isEmpty()) {
            return null;
        }
        DataStreamOptions.Builder builder = null;
        for (ResettableValue<DataStreamOptions.Template> current : dataStreamOptionsList) {
            if (current.isDefined() == false) {
                continue;
            }
            if (current.shouldReset()) {
                builder = null;
            } else {
                DataStreamOptions.Template currentTemplate = current.get();
                if (builder == null) {
                    builder = DataStreamOptions.builder(currentTemplate);
                } else {
                    builder.composeTemplate(currentTemplate);
                }
            }
        }
        return builder;
    }

    /**
     * Given a state and a composable template, validate that the final composite template
     * generated by the composable template and all of its component templates contains valid
     * settings, mappings, and aliases.
     */
    private static void validateCompositeTemplate(
        final ProjectMetadata project,
        final String templateName,
        final ComposableIndexTemplate template,
        final IndicesService indicesService,
        final NamedXContentRegistry xContentRegistry,
        final SystemIndices systemIndices
    ) throws Exception {
        final String temporaryIndexName = "validate-template-" + UUIDs.randomBase64UUID().toLowerCase(Locale.ROOT);
        Settings resolvedSettings = resolveSettings(template, project.componentTemplates());

        // use the provided values, otherwise just pick valid dummy values
        int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(resolvedSettings);
        int dummyShards = resolvedSettings.getAsInt(
            IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1
        );
        int shardReplicas = resolvedSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        // Create the final aggregate settings, which will be used to create the temporary index metadata to validate everything
        Settings finalResolvedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(resolvedSettings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, shardReplicas)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();

        // Validate index metadata (settings)
        final IndexMetadata tmpIndexMetadata = IndexMetadata.builder(temporaryIndexName).settings(finalResolvedSettings).build();
        indicesService.withTempIndexService(tmpIndexMetadata, tempIndexService -> {
            // Validate aliases
            MetadataCreateIndexService.resolveAndValidateAliases(
                temporaryIndexName,
                Collections.emptySet(),
                MetadataIndexTemplateService.resolveAliases(project, template),
                project,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry,
                tempIndexService.newSearchExecutionContext(0, 0, null, () -> 0L, null, emptyMap()),
                IndexService.dateMathExpressionResolverAt(System.currentTimeMillis()),
                systemIndices::isSystemName
            );

            // triggers inclusion of _timestamp field and its validation:
            String indexName = DataStream.BACKING_INDEX_PREFIX + temporaryIndexName;
            // Parse mappings to ensure they are valid after being composed

            List<CompressedXContent> mappings = collectMappings(template, project.componentTemplates(), indexName);
            try {
                MapperService mapperService = tempIndexService.mapperService();
                mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappings, MapperService.MergeReason.INDEX_TEMPLATE);

                if (template.getDataStreamTemplate() != null) {
                    validateTimestampFieldMapping(mapperService.mappingLookup());
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("invalid composite mappings for [" + templateName + "]", e);
            }
            return null;
        });
    }

    public static void validateTemplate(Settings validateSettings, CompressedXContent mappings, IndicesService indicesService)
        throws Exception {
        // Hard to validate settings if they're non-existent, so used empty ones if none were provided
        Settings settings = validateSettings;
        if (settings == null) {
            settings = Settings.EMPTY;
        }

        final String temporaryIndexName = UUIDs.randomBase64UUID();
        int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(settings);
        int dummyShards = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1);
        int shardReplicas = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        // create index service for parsing and validating "mappings"
        Settings dummySettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(settings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, shardReplicas)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();

        final IndexMetadata tmpIndexMetadata = IndexMetadata.builder(temporaryIndexName).settings(dummySettings).build();

        indicesService.withTempIndexService(tmpIndexMetadata, dummyIndexService -> {
            if (mappings != null) {
                dummyIndexService.mapperService().merge(MapperService.SINGLE_MAPPING_NAME, mappings, MergeReason.INDEX_TEMPLATE);
            }
            return null;
        });
    }

    private void validate(String name, ComposableIndexTemplate template, @Nullable Settings systemProvided) {
        validate(name, template.template(), template.indexPatterns(), systemProvided);
    }

    private void validate(String name, Template template, List<String> indexPatterns, @Nullable Settings systemProvided) {
        Optional<Template> maybeTemplate = Optional.ofNullable(template);
        validate(
            name,
            maybeTemplate.map(Template::settings).orElse(Settings.EMPTY),
            systemProvided,
            indexPatterns,
            maybeTemplate.map(Template::aliases).orElse(emptyMap()).values().stream().map(MetadataIndexTemplateService::toAlias).toList()
        );
    }

    private static Alias toAlias(AliasMetadata aliasMeta) {
        Alias a = new Alias(aliasMeta.alias());
        if (aliasMeta.filter() != null) {
            a.filter(aliasMeta.filter().string());
        }
        a.searchRouting(aliasMeta.searchRouting());
        a.indexRouting(aliasMeta.indexRouting());
        a.isHidden(aliasMeta.isHidden());
        a.writeIndex(aliasMeta.writeIndex());
        return a;
    }

    private void validate(PutRequest putRequest) {
        validate(putRequest.name, putRequest.settings, null, putRequest.indexPatterns, putRequest.aliases);
    }

    private void validate(
        String name,
        @Nullable Settings settings,
        @Nullable Settings systemProvided,
        List<String> indexPatterns,
        List<Alias> aliases
    ) {
        List<String> validationErrors = new ArrayList<>();
        if (name.contains(" ")) {
            validationErrors.add("name must not contain a space");
        }
        if (name.contains(",")) {
            validationErrors.add("name must not contain a ','");
        }
        if (name.contains("#")) {
            validationErrors.add("name must not contain a '#'");
        }
        if (name.contains("*")) {
            validationErrors.add("name must not contain a '*'");
        }
        if (name.startsWith("_")) {
            validationErrors.add("name must not start with '_'");
        }
        if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
            validationErrors.add("name must be lower cased");
        }
        for (String indexPattern : indexPatterns) {
            if (indexPattern.contains(" ")) {
                validationErrors.add("index_patterns [" + indexPattern + "] must not contain a space");
            }
            if (indexPattern.contains(",")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain a ','");
            }
            if (indexPattern.contains("#")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain a '#'");
            }
            if (indexPattern.contains(":")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain a ':'");
            }
            if (indexPattern.startsWith("_")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not start with '_'");
            }
            if (Strings.validFileNameExcludingAstrix(indexPattern) == false) {
                validationErrors.add(
                    "index_pattern [" + indexPattern + "] must not contain the following characters " + Strings.INVALID_FILENAME_CHARS
                );
            }
        }

        if (settings != null) {
            try {
                // templates must be consistent with regards to dependencies
                indexScopedSettings.validate(settings, true);
            } catch (IllegalArgumentException iae) {
                validationErrors.add(iae.getMessage());
                for (Throwable t : iae.getSuppressed()) {
                    validationErrors.add(t.getMessage());
                }
            }
            List<String> indexSettingsValidation = metadataCreateIndexService.getIndexSettingsValidationErrors(
                settings,
                systemProvided,
                true
            );
            validationErrors.addAll(indexSettingsValidation);
        }

        if (indexPatterns.stream().anyMatch(Regex::isMatchAllPattern)) {
            if (settings != null && IndexMetadata.INDEX_HIDDEN_SETTING.exists(settings)) {
                validationErrors.add("global templates may not specify the setting " + IndexMetadata.INDEX_HIDDEN_SETTING.getKey());
            }
        }

        if (validationErrors.size() > 0) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(name, validationException.getMessage());
        }

        for (Alias alias : aliases) {
            // we validate the alias only partially, as we don't know yet to which index it'll get applied to
            AliasValidator.validateAliasStandalone(alias);
            if (indexPatterns.contains(alias.name())) {
                throw new IllegalArgumentException(
                    "alias [" + alias.name() + "] cannot be the same as any pattern in [" + String.join(", ", indexPatterns) + "]"
                );
            }
        }
    }

    public static class PutRequest {
        final String name;
        final String cause;
        boolean create;
        int order;
        Integer version;
        List<String> indexPatterns;
        Settings settings = Settings.EMPTY;
        CompressedXContent mappings = null;
        List<Alias> aliases = new ArrayList<>();

        public PutRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }

        public PutRequest order(int order) {
            this.order = order;
            return this;
        }

        public PutRequest patterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }

        public PutRequest create(boolean create) {
            this.create = create;
            return this;
        }

        public PutRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public PutRequest mappings(CompressedXContent mappings) {
            this.mappings = mappings;
            return this;
        }

        public PutRequest aliases(Set<Alias> aliases) {
            this.aliases.addAll(aliases);
            return this;
        }

        public PutRequest version(Integer version) {
            this.version = version;
            return this;
        }
    }
}
