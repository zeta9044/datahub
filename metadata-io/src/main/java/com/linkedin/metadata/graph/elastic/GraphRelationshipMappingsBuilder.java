package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.aspect.models.graph.Edge.*;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphRelationshipMappingsBuilder {

  private GraphRelationshipMappingsBuilder() {}

  public static Map<String, Object> getMappings() {
    Map<String, Object> mappings = new HashMap<>();
    mappings.put(EDGE_FIELD_SOURCE, getMappingsForEntity());
    mappings.put(EDGE_FIELD_DESTINATION, getMappingsForEntity());
    mappings.put(EDGE_FIELD_RELNSHIP_TYPE, getMappingsForKeyword());
    mappings.put(EDGE_FIELD_PROPERTIES, getMappingsForEdgeProperties());
    mappings.put(EDGE_FIELD_LIFECYCLE_OWNER, getMappingsForKeyword());
    mappings.put(EDGE_FIELD_VIA, getMappingsForKeyword());
    return ImmutableMap.of("properties", mappings);
  }

  private static Map<String, Object> getMappingsForKeyword() {
    return ImmutableMap.<String, Object>builder().put("type", "keyword").build();
  }

  private static Map<String, Object> getMappingsForEntity() {

    Map<String, Object> mappings =
        ImmutableMap.<String, Object>builder()
            .put("urn", getMappingsForKeyword())
            .put("entityType", getMappingsForKeyword())
            .build();

    return ImmutableMap.of("properties", mappings);
  }

  private static Map<String, Object> getMappingsForEdgeProperties() {

    Map<String, Object> propertyMappings =
        ImmutableMap.<String, Object>builder().put("source", getMappingsForKeyword()).build();

    return ImmutableMap.of("properties", propertyMappings);
  }
}
