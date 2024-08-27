package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.graph.elastic.ESGraphQueryDAO.buildQuery;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;

@Slf4j
@RequiredArgsConstructor
public class ESGraphWriteDAO {
  private final IndexConvention indexConvention;
  private final ESBulkProcessor bulkProcessor;
  private final int numRetries;

  private static final String ES_WRITES_METRIC = "num_elasticSearch_writes";

  /**
   * Updates or inserts the given search document.
   *
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocument(@Nonnull String docId, @Nonnull String document) {
    final UpdateRequest updateRequest =
        new UpdateRequest(indexConvention.getIndexName(INDEX_NAME), docId)
            .detectNoop(false)
            .docAsUpsert(true)
            .doc(document, XContentType.JSON)
            .retryOnConflict(numRetries);
    bulkProcessor.add(updateRequest);
  }

  /**
   * Deletes the given search document.
   *
   * @param docId the ID of the document
   */
  public void deleteDocument(@Nonnull String docId) {
    final DeleteRequest deleteRequest =
        new DeleteRequest(indexConvention.getIndexName(INDEX_NAME)).id(docId);
    bulkProcessor.add(deleteRequest);
  }

  public BulkByScrollResponse deleteByQuery(
      @Nullable final String sourceType,
      @Nonnull final Filter sourceEntityFilter,
      @Nullable final String destinationType,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter) {
    BoolQueryBuilder finalQuery =
        buildQuery(
            sourceType == null ? ImmutableList.of() : ImmutableList.of(sourceType),
            sourceEntityFilter,
            destinationType == null ? ImmutableList.of() : ImmutableList.of(destinationType),
            destinationEntityFilter,
            relationshipTypes,
            relationshipFilter);

    return bulkProcessor
        .deleteByQuery(finalQuery, indexConvention.getIndexName(INDEX_NAME))
        .orElse(null);
  }

  public BulkByScrollResponse deleteByQuery(
      @Nullable final String sourceType,
      @Nonnull final Filter sourceEntityFilter,
      @Nullable final String destinationType,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter,
      String lifecycleOwner) {
    BoolQueryBuilder finalQuery =
        buildQuery(
            sourceType == null ? ImmutableList.of() : ImmutableList.of(sourceType),
            sourceEntityFilter,
            destinationType == null ? ImmutableList.of() : ImmutableList.of(destinationType),
            destinationEntityFilter,
            relationshipTypes,
            relationshipFilter,
            lifecycleOwner);

    return bulkProcessor
        .deleteByQuery(finalQuery, indexConvention.getIndexName(INDEX_NAME))
        .orElse(null);
  }
}
