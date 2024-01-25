/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.DatasetVersionDatasetFacet;
import io.openlineage.client.OpenLineage.DatasourceDatasetFacet;
import io.openlineage.client.OpenLineage.DocumentationDatasetFacet;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet;
import io.openlineage.client.OpenLineage.OwnershipDatasetFacet;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineage.StorageDatasetFacet;
import io.openlineage.client.OpenLineage.SymlinksDatasetFacet;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.utils.DatasetIdentifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/** Utility class to load serialized facets json string into a dataset builder */
@Slf4j
class BuiltInDataSourceV2Utils {

  private static Map<String, TypeReference> predefinedFacets =
      new HashMap<String, TypeReference>() {
        {
          put("documentation", new TypeReference<DocumentationDatasetFacet>() {});
          put("dataSource", new TypeReference<DatasourceDatasetFacet>() {});
          put("version", new TypeReference<DatasetVersionDatasetFacet>() {});
          put("schema", new TypeReference<SchemaDatasetFacet>() {});
          put("ownership", new TypeReference<OwnershipDatasetFacet>() {});
          put("storage", new TypeReference<StorageDatasetFacet>() {});
          put("columnLineage", new TypeReference<ColumnLineageDatasetFacet>() {});
          put("symlinks", new TypeReference<SymlinksDatasetFacet>() {});
          put("lifecycleStateChange", new TypeReference<LifecycleStateChangeDatasetFacet>() {});
        }
      };

  /**
   * Given a table properties, it adds facets to builders from string representation within
   * properties
   */
  public static void loadBuilder(
      OpenLineage.DatasetFacetsBuilder builder, DataSourceV2Relation relation) {
    Map<String, String> properties = relation.table().properties();

    predefinedFacets.keySet().stream()
        .filter(field -> properties.containsKey("openlineage.dataset.facets." + field))
        .forEach(
            field -> {
              try {
                Object o =
                    OpenLineageClientUtils.fromJson(
                        properties.get("openlineage.dataset.facets." + field),
                        predefinedFacets.get(field));
                FieldUtils.writeField(builder, field, o, true);
              } catch (IllegalAccessException | RuntimeException e) {
                log.warn("Couldn't serialize and assign facet", e);
              }
            });

    // custom facets
    properties.keySet().stream()
        .filter(key -> key.startsWith("openlineage.dataset.facets."))
        .map(key -> StringUtils.substringAfterLast(key, "."))
        .filter(key -> !predefinedFacets.containsKey(key))
        .forEach(
            key -> {
              try {
                builder.put(
                    key,
                    OpenLineageClientUtils.fromJson(
                        properties.get("openlineage.dataset.facets." + key),
                        new TypeReference<DatasetFacet>() {}));
              } catch (RuntimeException e) {
                log.warn("Couldn't serialize and assign facet", e);
              }
            });
  }

  public static DatasetIdentifier getDatasetIdentifier(DataSourceV2Relation relation) {
    return new DatasetIdentifier(
        relation.table().properties().get("openlineage.dataset.name"),
        relation.table().properties().get("openlineage.dataset.namespace"));
  }

  public static boolean hasBuiltInLineage(DataSourceV2Relation relation) {
    return Optional.ofNullable(relation)
        .map(r -> r.table())
        .map(table -> table.properties())
        .filter(Objects::nonNull)
        .filter(props -> props.containsKey("openlineage.dataset.name"))
        .filter(props -> props.containsKey("openlineage.dataset.namespace"))
        .isPresent();
  }
}
