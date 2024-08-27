package com.linkedin.metadata.search.query.request;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.custom.AutocompleteConfiguration;
import com.linkedin.metadata.config.search.custom.BoolQueryConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.lucene.search.function.FieldValueFactorFunction;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.testng.annotations.Test;

public class AutocompleteRequestHandlerTest {
  private AutocompleteRequestHandler handler =
      AutocompleteRequestHandler.getBuilder(
          TestEntitySpecBuilder.getSpec(), CustomSearchConfiguration.builder().build());
  private OperationContext mockOpContext =
      TestOperationContexts.systemContextNoSearchAuthorization(mock(EntityRegistry.class));

  private static final QueryConfiguration TEST_QUERY_CONFIG =
      QueryConfiguration.builder()
          .queryRegex(".*")
          .simpleQuery(true)
          .exactMatchQuery(true)
          .prefixMatchQuery(true)
          .boolQuery(
              BoolQueryConfiguration.builder()
                  .must(List.of(Map.of("term", Map.of("name", "{{query_string}}"))))
                  .build())
          .functionScore(
              Map.of(
                  "score_mode",
                  "avg",
                  "boost_mode",
                  "multiply",
                  "functions",
                  List.of(
                      Map.of(
                          "weight",
                          1,
                          "filter",
                          Map.<String, Object>of("match_all", Map.<String, Object>of())),
                      Map.of(
                          "weight",
                          0.5,
                          "filter",
                          Map.<String, Object>of(
                              "term", Map.of("materialized", Map.of("value", true)))),
                      Map.of(
                          "weight",
                          1.5,
                          "filter",
                          Map.<String, Object>of(
                              "term",
                              Map.<String, Object>of("deprecated", Map.of("value", false)))))))
          .build();

  @Test
  public void testDefaultAutocompleteRequest() {
    // When field is null
    SearchRequest autocompleteRequest =
        handler.getSearchRequest(mockOpContext, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    assertEquals(sourceBuilder.size(), 10);
    BoolQueryBuilder wrapper =
        (BoolQueryBuilder) ((FunctionScoreQueryBuilder) sourceBuilder.query()).query();
    BoolQueryBuilder query = (BoolQueryBuilder) extractNestedQuery(wrapper);
    assertEquals(query.should().size(), 3);

    MultiMatchQueryBuilder autocompleteQuery = (MultiMatchQueryBuilder) query.should().get(2);
    Map<String, Float> queryFields = autocompleteQuery.fields();
    assertTrue(queryFields.containsKey("keyPart1.ngram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._2gram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._3gram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._4gram"));
    assertEquals(autocompleteQuery.type(), MultiMatchQueryBuilder.Type.BOOL_PREFIX);

    MatchPhrasePrefixQueryBuilder prefixQuery =
        (MatchPhrasePrefixQueryBuilder) query.should().get(0);
    assertEquals("keyPart1.delimited", prefixQuery.fieldName());

    assertEquals(wrapper.mustNot().size(), 1);
    MatchQueryBuilder removedFilter = (MatchQueryBuilder) wrapper.mustNot().get(0);
    assertEquals(removedFilter.fieldName(), "removed");
    assertEquals(removedFilter.value(), true);
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<HighlightBuilder.Field> highlightedFields = highlightBuilder.fields();
    assertEquals(highlightedFields.size(), 8);
    assertEquals(highlightedFields.get(0).name(), "keyPart1");
    assertEquals(highlightedFields.get(1).name(), "keyPart1.*");
    assertEquals(highlightedFields.get(2).name(), "keyPart1.ngram");
    assertEquals(highlightedFields.get(3).name(), "keyPart1.delimited");
    assertEquals(highlightedFields.get(4).name(), "urn");
    assertEquals(highlightedFields.get(5).name(), "urn.*");
    assertEquals(highlightedFields.get(6).name(), "urn.ngram");
    assertEquals(highlightedFields.get(7).name(), "urn.delimited");
  }

  @Test
  public void testAutocompleteRequestWithField() {
    // When field is null
    SearchRequest autocompleteRequest =
        handler.getSearchRequest(mockOpContext, "input", "field", null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    assertEquals(sourceBuilder.size(), 10);
    BoolQueryBuilder wrapper =
        (BoolQueryBuilder) ((FunctionScoreQueryBuilder) sourceBuilder.query()).query();
    assertEquals(wrapper.should().size(), 1);
    BoolQueryBuilder query = (BoolQueryBuilder) extractNestedQuery(wrapper);
    assertEquals(query.should().size(), 2);

    MultiMatchQueryBuilder autocompleteQuery = (MultiMatchQueryBuilder) query.should().get(1);
    Map<String, Float> queryFields = autocompleteQuery.fields();
    assertTrue(queryFields.containsKey("field.ngram"));
    assertTrue(queryFields.containsKey("field.ngram._2gram"));
    assertTrue(queryFields.containsKey("field.ngram._3gram"));
    assertTrue(queryFields.containsKey("field.ngram._4gram"));
    assertEquals(autocompleteQuery.type(), MultiMatchQueryBuilder.Type.BOOL_PREFIX);

    MatchPhrasePrefixQueryBuilder prefixQuery =
        (MatchPhrasePrefixQueryBuilder) query.should().get(0);
    assertEquals("field.delimited", prefixQuery.fieldName());

    MatchQueryBuilder removedFilter = (MatchQueryBuilder) wrapper.mustNot().get(0);
    assertEquals(removedFilter.fieldName(), "removed");
    assertEquals(removedFilter.value(), true);
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<HighlightBuilder.Field> highlightedFields = highlightBuilder.fields();
    assertEquals(highlightedFields.size(), 4);
    assertEquals(highlightedFields.get(0).name(), "field");
    assertEquals(highlightedFields.get(1).name(), "field.*");
    assertEquals(highlightedFields.get(2).name(), "field.ngram");
    assertEquals(highlightedFields.get(3).name(), "field.delimited");
  }

  @Test
  public void testCustomConfigWithDefault() {
    // Exclude Default query
    AutocompleteRequestHandler withoutDefaultQuery =
        AutocompleteRequestHandler.getBuilder(
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(false)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .build()))
                .build());

    SearchRequest autocompleteRequest =
        withoutDefaultQuery.getSearchRequest(mockOpContext, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    FunctionScoreQueryBuilder wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    assertEquals(((BoolQueryBuilder) wrapper.query()).should().size(), 1);
    QueryBuilder customQuery = extractNestedQuery((BoolQueryBuilder) wrapper.query());
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    // Include Default query
    AutocompleteRequestHandler withDefaultQuery =
        AutocompleteRequestHandler.getBuilder(
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(true)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .build()))
                .build());

    autocompleteRequest = withDefaultQuery.getSearchRequest(mockOpContext, "input", null, null, 10);
    sourceBuilder = autocompleteRequest.source();
    wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    BoolQueryBuilder query =
        ((BoolQueryBuilder) ((BoolQueryBuilder) wrapper.query()).should().get(0));
    assertEquals(query.should().size(), 2);

    List<QueryBuilder> shouldQueries = query.should();

    // Default
    BoolQueryBuilder defaultQuery =
        (BoolQueryBuilder)
            shouldQueries.stream().filter(qb -> qb instanceof BoolQueryBuilder).findFirst().get();
    assertEquals(defaultQuery.should().size(), 3);

    // Custom
    customQuery =
        shouldQueries.stream().filter(qb -> qb instanceof MatchAllQueryBuilder).findFirst().get();
    assertEquals(customQuery, QueryBuilders.matchAllQuery());
  }

  @Test
  public void testCustomConfigWithInheritedQueryFunctionScores() {
    // Pickup scoring functions from non-autocomplete
    AutocompleteRequestHandler withInherit =
        AutocompleteRequestHandler.getBuilder(
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .queryConfigurations(List.of(TEST_QUERY_CONFIG))
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(false)
                            .inheritFunctionScore(true)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .build()))
                .build());

    SearchRequest autocompleteRequest =
        withInherit.getSearchRequest(mockOpContext, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    FunctionScoreQueryBuilder wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    assertEquals(((BoolQueryBuilder) wrapper.query()).should().size(), 1);

    QueryBuilder customQuery = extractNestedQuery(((BoolQueryBuilder) wrapper.query()));
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedQueryConfigurationScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.weightFactorFunction(1f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("materialized", true),
          ScoreFunctionBuilders.weightFactorFunction(0.5f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("deprecated", false),
          ScoreFunctionBuilders.weightFactorFunction(1.5f))
    };
    assertEquals(wrapper.filterFunctionBuilders(), expectedQueryConfigurationScoreFunctions);

    // no search query customization
    AutocompleteRequestHandler noQueryCustomization =
        AutocompleteRequestHandler.getBuilder(
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(false)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .build()))
                .build());

    autocompleteRequest =
        noQueryCustomization.getSearchRequest(mockOpContext, "input", null, null, 10);
    sourceBuilder = autocompleteRequest.source();
    wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    assertEquals(((BoolQueryBuilder) wrapper.query()).should().size(), 1);

    customQuery = extractNestedQuery((BoolQueryBuilder) wrapper.query());
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    // PDL annotation based on default behavior of query builder
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedDefaultScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.weightFactorFunction(1f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.fieldValueFactorFunction("feature2")
              .modifier(FieldValueFactorFunction.Modifier.NONE)
              .missing(0.0)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.fieldValueFactorFunction("feature1")
              .modifier(FieldValueFactorFunction.Modifier.LOG1P)
              .missing(0.0))
    };
    assertEquals(wrapper.filterFunctionBuilders(), expectedDefaultScoreFunctions);
  }

  @Test
  public void testCustomConfigWithFunctionScores() {
    // Scoring functions explicit autocomplete override
    AutocompleteRequestHandler explicitNoInherit =
        AutocompleteRequestHandler.getBuilder(
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .queryConfigurations(List.of(TEST_QUERY_CONFIG)) // should be ignored
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(false)
                            .inheritFunctionScore(false)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .functionScore(
                                Map.of(
                                    "score_mode",
                                    "avg",
                                    "boost_mode",
                                    "multiply",
                                    "functions",
                                    List.of(
                                        Map.of(
                                            "weight",
                                            1.5,
                                            "filter",
                                            Map.<String, Object>of(
                                                "term",
                                                Map.<String, Object>of(
                                                    "deprecated", Map.of("value", false)))))))
                            .build()))
                .build());

    SearchRequest autocompleteRequest =
        explicitNoInherit.getSearchRequest(mockOpContext, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    FunctionScoreQueryBuilder wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    assertEquals(((BoolQueryBuilder) wrapper.query()).should().size(), 1);

    QueryBuilder customQuery = extractNestedQuery((BoolQueryBuilder) wrapper.query());
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedCustomScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("deprecated", false),
          ScoreFunctionBuilders.weightFactorFunction(1.5f))
    };
    assertEquals(wrapper.filterFunctionBuilders(), expectedCustomScoreFunctions);

    // Pickup scoring functions explicit autocomplete override (even though default query and
    // inherit enabled)
    AutocompleteRequestHandler explicit =
        AutocompleteRequestHandler.getBuilder(
            TestEntitySpecBuilder.getSpec(),
            CustomSearchConfiguration.builder()
                .queryConfigurations(List.of(TEST_QUERY_CONFIG)) // should be ignored
                .autocompleteConfigurations(
                    List.of(
                        AutocompleteConfiguration.builder()
                            .queryRegex(".*")
                            .defaultQuery(true)
                            .inheritFunctionScore(true)
                            .boolQuery(
                                BoolQueryConfiguration.builder()
                                    .should(List.of(Map.of("match_all", Map.of())))
                                    .build())
                            .functionScore(
                                Map.of(
                                    "score_mode",
                                    "avg",
                                    "boost_mode",
                                    "multiply",
                                    "functions",
                                    List.of(
                                        Map.of(
                                            "weight",
                                            1.5,
                                            "filter",
                                            Map.<String, Object>of(
                                                "term",
                                                Map.<String, Object>of(
                                                    "deprecated", Map.of("value", false)))))))
                            .build()))
                .build());

    autocompleteRequest = explicit.getSearchRequest(mockOpContext, "input", null, null, 10);
    sourceBuilder = autocompleteRequest.source();
    wrapper = (FunctionScoreQueryBuilder) sourceBuilder.query();
    BoolQueryBuilder query =
        ((BoolQueryBuilder) ((BoolQueryBuilder) wrapper.query()).should().get(0));
    assertEquals(query.should().size(), 2);

    customQuery = query.should().get(0);
    assertEquals(customQuery, QueryBuilders.matchAllQuery());

    // standard query still present
    assertEquals(((BoolQueryBuilder) query.should().get(1)).should().size(), 3);

    // custom functions included
    assertEquals(wrapper.filterFunctionBuilders(), expectedCustomScoreFunctions);
  }

  private static QueryBuilder extractNestedQuery(BoolQueryBuilder nested) {
    assertEquals(nested.should().size(), 1);
    BoolQueryBuilder firstLevel = (BoolQueryBuilder) nested.should().get(0);
    assertEquals(firstLevel.should().size(), 1);
    return firstLevel.should().get(0);
  }
}
