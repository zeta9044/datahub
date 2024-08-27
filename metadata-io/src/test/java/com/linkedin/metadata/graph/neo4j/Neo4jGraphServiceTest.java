package com.linkedin.metadata.graph.neo4j;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.FabricType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.GraphServiceTestBase;
import com.linkedin.metadata.graph.GraphServiceTestBaseNoVia;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class Neo4jGraphServiceTest extends GraphServiceTestBaseNoVia {

  private Neo4jTestServerBuilder _serverBuilder;
  private Driver _driver;
  private Neo4jGraphService _client;

  private static final String TAG_RELATIONSHIP = "SchemaFieldTaggedWith";

  @BeforeClass
  public void init() {
    _serverBuilder = new Neo4jTestServerBuilder();
    _serverBuilder.newServer();
    _driver = GraphDatabase.driver(_serverBuilder.boltURI());
    _client =
        new Neo4jGraphService(new LineageRegistry(SnapshotEntityRegistry.getInstance()), _driver);
    _client.clear();
  }

  @BeforeMethod
  public void wipe() {
    _client.wipe();
  }

  @AfterClass
  public void tearDown() {
    _serverBuilder.shutdown();
  }

  @Override
  protected @Nonnull GraphService getGraphService() {
    return _client;
  }

  @Override
  protected void syncAfterWrite() {}

  @Override
  protected void assertEqualsAnyOrder(
      RelatedEntitiesResult actual, RelatedEntitiesResult expected) {
    // https://github.com/datahub-project/datahub/issues/3118
    // Neo4jGraphService produces duplicates, which is here ignored until fixed
    // actual.count and actual.total not tested due to duplicates
    assertEquals(actual.getStart(), expected.getStart());
    assertEqualsAnyOrder(actual.getEntities(), expected.getEntities(), RELATED_ENTITY_COMPARATOR);
  }

  @Override
  protected <T> void assertEqualsAnyOrder(
      List<T> actual, List<T> expected, Comparator<T> comparator) {
    // https://github.com/datahub-project/datahub/issues/3118
    // Neo4jGraphService produces duplicates, which is here ignored until fixed
    assertEquals(new HashSet<>(actual), new HashSet<>(expected));
  }

  @Override
  @Test(dataProvider = "NoViaFindRelatedEntitiesSourceTypeTests")
  public void testFindRelatedEntitiesSourceType(
      String datasetType,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3119
      throw new SkipException("Neo4jGraphService does not support empty source type");
    }
    if (datasetType != null && datasetType.equals(GraphServiceTestBase.userType)) {
      // https://github.com/datahub-project/datahub/issues/3123
      // only test cases with "user" type fail due to this bug
      throw new SkipException("Neo4jGraphService does not apply source / destination types");
    }
    super.testFindRelatedEntitiesSourceType(
        datasetType, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Override
  @Test(dataProvider = "NoViaFindRelatedEntitiesDestinationTypeTests")
  public void testFindRelatedEntitiesDestinationType(
      String datasetType,
      List<String> relationshipTypes,
      RelationshipFilter relationships,
      List<RelatedEntity> expectedRelatedEntities)
      throws Exception {
    if (datasetType != null && datasetType.isEmpty()) {
      // https://github.com/datahub-project/datahub/issues/3119
      throw new SkipException("Neo4jGraphService does not support empty destination type");
    }
    if (relationshipTypes.contains(hasOwner)) {
      // https://github.com/datahub-project/datahub/issues/3123
      // only test cases with "HasOwner" relatioship fail due to this bug
      throw new SkipException("Neo4jGraphService does not apply source / destination types");
    }
    super.testFindRelatedEntitiesDestinationType(
        datasetType, relationshipTypes, relationships, expectedRelatedEntities);
  }

  @Test
  @Override
  public void testFindRelatedEntitiesNullSourceType() throws Exception {
    // https://github.com/datahub-project/datahub/issues/3121
    throw new SkipException("Neo4jGraphService does not support 'null' entity type string");
  }

  @Test
  @Override
  public void testFindRelatedEntitiesNullDestinationType() throws Exception {
    // https://github.com/datahub-project/datahub/issues/3121
    throw new SkipException("Neo4jGraphService does not support 'null' entity type string");
  }

  @Test
  @Override
  public void testFindRelatedEntitiesNoRelationshipTypes() {
    // https://github.com/datahub-project/datahub/issues/3120
    throw new SkipException("Neo4jGraphService does not support empty list of relationship types");
  }

  @Test
  @Override
  public void testRemoveEdgesFromNodeNoRelationshipTypes() {
    // https://github.com/datahub-project/datahub/issues/3120
    throw new SkipException("Neo4jGraphService does not support empty list of relationship types");
  }

  @Test
  @Override
  public void testConcurrentAddEdge() {
    // https://github.com/datahub-project/datahub/issues/3141
    throw new SkipException(
        "Neo4jGraphService does not manage to add all edges added concurrently");
  }

  @Test
  public void testRemoveEdge() throws Exception {
    DatasetUrn datasetUrn =
        new DatasetUrn(new DataPlatformUrn("snowflake"), "test", FabricType.TEST);
    TagUrn tagUrn = new TagUrn("newTag");
    Edge edge = new Edge(datasetUrn, tagUrn, TAG_RELATIONSHIP, null, null, null, null, null);
    getGraphService().addEdge(edge);

    RelatedEntitiesResult result =
        getGraphService()
            .findRelatedEntities(
                Collections.singletonList(datasetType),
                newFilter(Collections.singletonMap("urn", datasetUrn.toString())),
                Collections.singletonList("tag"),
                EMPTY_FILTER,
                Collections.singletonList(TAG_RELATIONSHIP),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING),
                0,
                100);
    assertEquals(result.getTotal(), 1);
    getGraphService().removeEdge(edge);

    result =
        getGraphService()
            .findRelatedEntities(
                Collections.singletonList(datasetType),
                newFilter(Collections.singletonMap("urn", datasetUrn.toString())),
                Collections.singletonList("tag"),
                EMPTY_FILTER,
                Collections.singletonList(TAG_RELATIONSHIP),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING),
                0,
                100);
    assertEquals(result.getTotal(), 0);
  }

  private Set<UrnArray> getPathUrnArraysFromLineageResult(EntityLineageResult result) {
    return result.getRelationships().stream()
        .map(x -> x.getPaths().get(0))
        .collect(Collectors.toSet());
  }

  @Test
  public void testGetLineage() {
    GraphService service = getGraphService();

    List<Edge> edges =
        Arrays.asList(
            // d1 <-Consumes- dj1 -Produces-> d2 <-DownstreamOf- d3 <-DownstreamOf- d5
            new Edge(dataJobOneUrn, dataset1Urn, consumes, 1L, null, 3L, null, null),
            new Edge(dataJobOneUrn, dataset2Urn, produces, 5L, null, 7L, null, null),
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, 9L, null, null, null, null),
            new Edge(dataset5Urn, dataset3Urn, downstreamOf, 11L, null, null, null, null),

            // another path between d2 and d5 which is shorter
            // d1 <-DownstreamOf- d4 <-DownstreamOf- d5
            new Edge(dataset4Urn, dataset1Urn, downstreamOf, 13L, null, 13L, null, null),
            new Edge(dataset5Urn, dataset4Urn, downstreamOf, 13L, null, 13L, null, null));
    edges.forEach(service::addEdge);

    // simple path finding
    final var upstreamLineageDataset3Hop3 =
        service.getLineage(dataset3Urn, LineageDirection.UPSTREAM, 0, 1000, 3);
    assertEquals(upstreamLineageDataset3Hop3.getTotal().intValue(), 3);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageDataset3Hop3),
        Set.of(
            new UrnArray(dataset3Urn, dataset2Urn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn, dataset1Urn)));

    // simple path finding
    final var upstreamLineageDatasetFiveHop2 =
        service.getLineage(dataset5Urn, LineageDirection.UPSTREAM, 0, 1000, 2);
    assertEquals(upstreamLineageDatasetFiveHop2.getTotal().intValue(), 4);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageDatasetFiveHop2),
        Set.of(
            new UrnArray(dataset5Urn, dataset3Urn),
            new UrnArray(dataset5Urn, dataset3Urn, dataset2Urn),
            new UrnArray(dataset5Urn, dataset4Urn),
            new UrnArray(dataset5Urn, dataset4Urn, dataset1Urn)));

    // there are two paths from p5 to p1, one longer and one shorter, and the longer one is
    // discarded from result
    final var upstreamLineageDataset5Hop5 =
        service.getLineage(dataset5Urn, LineageDirection.UPSTREAM, 0, 1000, 5);
    assertEquals(upstreamLineageDataset5Hop5.getTotal().intValue(), 5);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageDataset5Hop5),
        Set.of(
            new UrnArray(dataset5Urn, dataset3Urn),
            new UrnArray(dataset5Urn, dataset3Urn, dataset2Urn),
            new UrnArray(dataset5Urn, dataset3Urn, dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset5Urn, dataset4Urn),
            new UrnArray(dataset5Urn, dataset4Urn, dataset1Urn)));

    // downstream lookup
    final var downstreamLineageDataset1Hop2 =
        service.getLineage(dataset1Urn, LineageDirection.DOWNSTREAM, 0, 1000, 2);
    assertEquals(downstreamLineageDataset1Hop2.getTotal().intValue(), 4);
    assertEquals(
        getPathUrnArraysFromLineageResult(downstreamLineageDataset1Hop2),
        Set.of(
            new UrnArray(dataset1Urn, dataJobOneUrn),
            new UrnArray(dataset1Urn, dataJobOneUrn, dataset2Urn),
            new UrnArray(dataset1Urn, dataset4Urn),
            new UrnArray(dataset1Urn, dataset4Urn, dataset5Urn)));
  }

  @Test
  public void testGetLineageTimeFilterQuery() throws Exception {
    GraphService service = getGraphService();

    List<Edge> edges =
        Arrays.asList(
            // d1 <-Consumes- dj1 -Produces-> d2 <-DownstreamOf- d3 <-DownstreamOf- d4
            new Edge(dataJobOneUrn, dataset1Urn, consumes, 1L, null, 3L, null, null),
            new Edge(dataJobOneUrn, dataset2Urn, produces, 5L, null, 7L, null, null),
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, 9L, null, null, null, null),
            new Edge(dataset4Urn, dataset3Urn, downstreamOf, 11L, null, null, null, null));
    edges.forEach(service::addEdge);

    // no time filtering
    EntityLineageResult upstreamLineageTwoHops =
        service.getLineage(dataset4Urn, LineageDirection.UPSTREAM, 0, 1000, 2);
    assertEquals(upstreamLineageTwoHops.getTotal().intValue(), 2);
    assertEquals(upstreamLineageTwoHops.getRelationships().size(), 2);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageTwoHops),
        Set.of(
            new UrnArray(dataset4Urn, dataset3Urn),
            new UrnArray(dataset4Urn, dataset3Urn, dataset2Urn)));

    // with time filtering
    EntityLineageResult upstreamLineageTwoHopsWithTimeFilter =
        service.getLineage(
            dataset4Urn,
            LineageDirection.UPSTREAM,
            0,
            1000,
            2,
            new LineageFlags().setStartTimeMillis(10L).setEndTimeMillis(12L));
    assertEquals(upstreamLineageTwoHopsWithTimeFilter.getTotal().intValue(), 1);
    assertEquals(upstreamLineageTwoHopsWithTimeFilter.getRelationships().size(), 1);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageTwoHopsWithTimeFilter),
        Set.of(new UrnArray(dataset4Urn, dataset3Urn)));

    // with time filtering
    EntityLineageResult upstreamLineageTimeFilter =
        service.getLineage(
            dataset2Urn,
            LineageDirection.UPSTREAM,
            0,
            1000,
            4,
            new LineageFlags().setStartTimeMillis(2L).setEndTimeMillis(6L));
    assertEquals(upstreamLineageTimeFilter.getTotal().intValue(), 2);
    assertEquals(upstreamLineageTimeFilter.getRelationships().size(), 2);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageTimeFilter),
        Set.of(
            new UrnArray(dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset2Urn, dataJobOneUrn, dataset1Urn)));

    // with time filtering
    EntityLineageResult downstreamLineageTimeFilter =
        service.getLineage(
            dataset1Urn,
            LineageDirection.DOWNSTREAM,
            0,
            1000,
            4,
            new LineageFlags().setStartTimeMillis(0L).setEndTimeMillis(4L));
    assertEquals(downstreamLineageTimeFilter.getTotal().intValue(), 1);
    assertEquals(downstreamLineageTimeFilter.getRelationships().size(), 1);
    assertEquals(
        getPathUrnArraysFromLineageResult(downstreamLineageTimeFilter),
        Set.of(new UrnArray(dataset1Urn, dataJobOneUrn)));
  }

  @Test
  public void testGetLineageTimeFilteringSkipsShorterButNonMatchingPaths() {
    GraphService service = getGraphService();

    List<Edge> edges =
        Arrays.asList(
            // d1 <-Consumes- dj1 -Produces-> d2 <-DownstreamOf- d3
            new Edge(dataJobOneUrn, dataset1Urn, consumes, 5L, null, 5L, null, null),
            new Edge(dataJobOneUrn, dataset2Urn, produces, 7L, null, 7L, null, null),
            new Edge(dataset3Urn, dataset2Urn, downstreamOf, 9L, null, null, null, null),

            // d1 <-DownstreamOf- d3 (shorter path from d3 to d1, but with very old time)
            new Edge(dataset3Urn, dataset1Urn, downstreamOf, 1L, null, 2L, null, null));
    edges.forEach(service::addEdge);

    // no time filtering, shorter path from d3 to d1 is returned
    EntityLineageResult upstreamLineageNoTimeFiltering =
        service.getLineage(dataset3Urn, LineageDirection.UPSTREAM, 0, 1000, 3);
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageNoTimeFiltering),
        Set.of(
            new UrnArray(dataset3Urn, dataset2Urn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset3Urn, dataset1Urn)));

    // with time filtering, shorter path from d3 to d1 is excluded so longer path is returned
    EntityLineageResult upstreamLineageTimeFiltering =
        service.getLineage(
            dataset3Urn,
            LineageDirection.UPSTREAM,
            0,
            1000,
            3,
            new LineageFlags().setStartTimeMillis(3L).setEndTimeMillis(17L));
    assertEquals(
        getPathUrnArraysFromLineageResult(upstreamLineageTimeFiltering),
        Set.of(
            new UrnArray(dataset3Urn, dataset2Urn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn),
            new UrnArray(dataset3Urn, dataset2Urn, dataJobOneUrn, dataset1Urn)));
  }

  @Override
  public void testHighlyConnectedGraphWalk() throws Exception {
    // TODO: explore limit not supported for Neo4J
  }
}
