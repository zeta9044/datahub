package com.linkedin.metadata.entity.validation;

import com.codahale.metrics.Timer;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.AbstractArrayTemplate;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.browse.BrowseResultEntityArray;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValidationUtils {

  public static SearchResult validateSearchResult(
      @Nonnull OperationContext opContext,
      final SearchResult searchResult,
      @Nonnull final EntityService<?> entityService) {
    try (Timer.Context ignored =
        MetricUtils.timer(ValidationUtils.class, "validateSearchResult").time()) {
      if (searchResult == null) {
        return null;
      }
      Objects.requireNonNull(entityService, "entityService must not be null");

      SearchResult validatedSearchResult =
          new SearchResult()
              .setFrom(searchResult.getFrom())
              .setMetadata(searchResult.getMetadata())
              .setPageSize(searchResult.getPageSize())
              .setNumEntities(searchResult.getNumEntities());

      SearchEntityArray validatedEntities =
          validateSearchUrns(
                  opContext,
                  searchResult.getEntities(),
                  SearchEntity::getEntity,
                  entityService,
                  true,
                  true)
              .collect(Collectors.toCollection(SearchEntityArray::new));
      validatedSearchResult.setEntities(validatedEntities);

      return validatedSearchResult;
    }
  }

  public static ScrollResult validateScrollResult(
      @Nonnull OperationContext opContext,
      final ScrollResult scrollResult,
      @Nonnull final EntityService<?> entityService) {
    if (scrollResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    ScrollResult validatedScrollResult =
        new ScrollResult()
            .setMetadata(scrollResult.getMetadata())
            .setPageSize(scrollResult.getPageSize())
            .setNumEntities(scrollResult.getNumEntities());
    if (scrollResult.getScrollId() != null) {
      validatedScrollResult.setScrollId(scrollResult.getScrollId());
    }

    SearchEntityArray validatedEntities =
        validateSearchUrns(
                opContext,
                scrollResult.getEntities(),
                SearchEntity::getEntity,
                entityService,
                true,
                true)
            .collect(Collectors.toCollection(SearchEntityArray::new));

    validatedScrollResult.setEntities(validatedEntities);

    return validatedScrollResult;
  }

  public static BrowseResult validateBrowseResult(
      @Nonnull OperationContext opContext,
      final BrowseResult browseResult,
      @Nonnull final EntityService<?> entityService) {
    try (Timer.Context ignored =
        MetricUtils.timer(ValidationUtils.class, "validateBrowseResult").time()) {
      if (browseResult == null) {
        return null;
      }
      Objects.requireNonNull(entityService, "entityService must not be null");

      BrowseResult validatedBrowseResult =
          new BrowseResult()
              .setGroups(browseResult.getGroups())
              .setMetadata(browseResult.getMetadata())
              .setFrom(browseResult.getFrom())
              .setPageSize(browseResult.getPageSize())
              .setNumGroups(browseResult.getNumGroups())
              .setNumEntities(browseResult.getNumEntities())
              .setNumElements(browseResult.getNumElements());

      BrowseResultEntityArray validatedEntities =
          validateSearchUrns(
                  opContext,
                  browseResult.getEntities(),
                  BrowseResultEntity::getUrn,
                  entityService,
                  true,
                  true)
              .collect(Collectors.toCollection(BrowseResultEntityArray::new));
      validatedBrowseResult.setEntities(validatedEntities);

      return validatedBrowseResult;
    }
  }

  public static ListResult validateListResult(
      @Nonnull OperationContext opContext,
      final ListResult listResult,
      @Nonnull final EntityService<?> entityService) {
    try (Timer.Context ignored =
        MetricUtils.timer(ValidationUtils.class, "validateListResult").time()) {
      if (listResult == null) {
        return null;
      }
      Objects.requireNonNull(entityService, "entityService must not be null");

      ListResult validatedListResult =
          new ListResult()
              .setStart(listResult.getStart())
              .setCount(listResult.getCount())
              .setTotal(listResult.getTotal());

      UrnArray validatedEntities =
          validateSearchUrns(
                  opContext,
                  listResult.getEntities(),
                  Function.identity(),
                  entityService,
                  true,
                  true)
              .collect(Collectors.toCollection(UrnArray::new));
      validatedListResult.setEntities(validatedEntities);

      return validatedListResult;
    }
  }

  public static LineageSearchResult validateLineageSearchResult(
      @Nonnull OperationContext opContext,
      final LineageSearchResult lineageSearchResult,
      @Nonnull final EntityService<?> entityService) {
    try (Timer.Context ignored =
        MetricUtils.timer(ValidationUtils.class, "validateLineageResult").time()) {
      if (lineageSearchResult == null) {
        return null;
      }
      Objects.requireNonNull(entityService, "entityService must not be null");

      LineageSearchResult validatedLineageSearchResult =
          new LineageSearchResult()
              .setMetadata(lineageSearchResult.getMetadata())
              .setFrom(lineageSearchResult.getFrom())
              .setPageSize(lineageSearchResult.getPageSize())
              .setNumEntities(lineageSearchResult.getNumEntities());

      LineageSearchEntityArray validatedEntities =
          validateSearchUrns(
                  opContext,
                  lineageSearchResult.getEntities(),
                  LineageSearchEntity::getEntity,
                  entityService,
                  true,
                  true)
              .collect(Collectors.toCollection(LineageSearchEntityArray::new));
      validatedLineageSearchResult.setEntities(validatedEntities);

      log.debug("Returning validated lineage search results");
      return validatedLineageSearchResult;
    }
  }

  public static EntityLineageResult validateEntityLineageResult(
      @Nonnull OperationContext opContext,
      @Nullable final EntityLineageResult entityLineageResult,
      @Nonnull final EntityService<?> entityService) {
    if (entityLineageResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    final EntityLineageResult validatedEntityLineageResult =
        new EntityLineageResult()
            .setStart(entityLineageResult.getStart())
            .setCount(entityLineageResult.getCount())
            .setTotal(entityLineageResult.getTotal());

    LineageRelationshipArray validatedRelationships =
        validateSearchUrns(
                opContext,
                entityLineageResult.getRelationships(),
                LineageRelationship::getEntity,
                entityService,
                true,
                false)
            .collect(Collectors.toCollection(LineageRelationshipArray::new));

    validatedEntityLineageResult.setFiltered(
        (entityLineageResult.hasFiltered() && entityLineageResult.getFiltered() != null
                ? entityLineageResult.getFiltered()
                : 0)
            + entityLineageResult.getRelationships().size()
            - validatedRelationships.size());
    validatedEntityLineageResult.setRelationships(validatedRelationships);

    return validatedEntityLineageResult;
  }

  public static LineageScrollResult validateLineageScrollResult(
      @Nonnull OperationContext opContext,
      final LineageScrollResult lineageScrollResult,
      @Nonnull final EntityService<?> entityService) {
    if (lineageScrollResult == null) {
      return null;
    }
    Objects.requireNonNull(entityService, "entityService must not be null");

    LineageScrollResult validatedLineageScrollResult =
        new LineageScrollResult()
            .setMetadata(lineageScrollResult.getMetadata())
            .setPageSize(lineageScrollResult.getPageSize())
            .setNumEntities(lineageScrollResult.getNumEntities());
    if (lineageScrollResult.getScrollId() != null) {
      validatedLineageScrollResult.setScrollId(lineageScrollResult.getScrollId());
    }

    LineageSearchEntityArray validatedEntities =
        validateSearchUrns(
                opContext,
                lineageScrollResult.getEntities(),
                LineageSearchEntity::getEntity,
                entityService,
                true,
                true)
            .collect(Collectors.toCollection(LineageSearchEntityArray::new));

    validatedLineageScrollResult.setEntities(validatedEntities);

    return validatedLineageScrollResult;
  }

  private static <T> Stream<T> validateSearchUrns(
      @Nonnull OperationContext opContext,
      final AbstractArrayTemplate<T> array,
      Function<T, Urn> urnFunction,
      @Nonnull final EntityService<?> entityService,
      boolean enforceSQLExistence,
      boolean includeSoftDeleted) {

    if (enforceSQLExistence) {
      Set<Urn> existingUrns =
          entityService.exists(
              opContext,
              array.stream().map(urnFunction).collect(Collectors.toList()),
              includeSoftDeleted);
      return array.stream().filter(item -> existingUrns.contains(urnFunction.apply(item)));
    } else {
      Set<Urn> validatedUrns =
          array.stream()
              .map(urnFunction)
              .filter(
                  urn -> {
                    try {
                      ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), urn);
                      return true;
                    } catch (Exception e) {
                      log.warn(
                          "Excluded {} from search result due to {}",
                          urn.toString(),
                          e.getMessage());
                    }
                    return false;
                  })
              .collect(Collectors.toSet());
      return array.stream().filter(item -> validatedUrns.contains(urnFunction.apply(item)));
    }
  }

  private ValidationUtils() {}
}
