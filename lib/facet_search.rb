class FacetSearch
  attr_accessor :klass, :schema, :search_params, :sort, :per_page, :page, :additional_query, :additional_must_queries

  def initialize
    @sort = nil
    @per_page = 20
    @page = 1
  end

  def search(query)
    klass.search(query)
  end

  def build_query
    query = {
      size: (@per_page.present? ? @per_page : 10000),
      from: (@per_page.present? ? (@page.to_i-1)*(@per_page.to_i) : 0)
    }
    query.deep_merge! search_query
    query.deep_merge!(sort_query) if @sort.present?
    query.deep_merge!(@additional_query) if @additional_query.present?
    if @additional_must_queries.present?
      if query.dig(:query, :bool, :must).present?
        query[:query][:bool][:must] = query[:query][:bool][:must].concat(@additional_must_queries)
      else
        query.deep_merge!({query: { bool: {must: @additional_must_queries }}})
      end
    end
    query
  end

  def hits
    return @hits if @hits
    es_result = search(build_query)
    results = es_result.records
    @total_page = ((es_result.response.hits.total.to_d || 0)/@per_page.to_d).ceil if @per_page.present?
    @total_hits = (es_result.response.hits.total || 0).to_i
    @hits = {
      objects: results,
      total_page: @total_page,
      total_hits: @total_hits
    }
  end

  def facets
    return @facets if @facets.present?
    facets = []
    @schema.each do |aggregation_name, schema|
      if schema[:facet]
        query = {size: 0}
        query.deep_merge! search_query(except: schema[:include_in_search] ? nil : aggregation_name)
        query[:aggs] = aggregation_query_for_field(aggregation_name, schema)
        if @additional_must_queries.present?
          if query.dig(:query, :bool, :must).present?
            query[:query][:bool][:must] = query[:query][:bool][:must].concat(@additional_must_queries)
          else
            query.deep_merge!({query: { bool: {must: @additional_must_queries }}})
          end
        end
        query_result = klass.search(query, size: 0)
        aggregation = query_result.aggregations[aggregation_name]
        if aggregation.present? && aggregation.buckets.any?
          facets.push(
            name: aggregation_name,
            options: aggregation.buckets.map{|b| b[:key]}
          )
        end
      else
        facets.push(
          name: aggregation_name
        )
      end
    end
    @facets = facets
  end

  def sort_query
    query = {sort: []}
    @sort.each do |k, v|
      query[:sort].push({
        "#{k.to_s}": {
          order: v
        }
      })
    end
    query
  end

  def search_query(except: nil)
    query = {query: {}}
    must_queries = []
    if @search_params
      @schema.each do |aggregation_name, schema|
        unless except && except.to_s == aggregation_name.to_s
          aggregation_search_query = search_query_for_field(aggregation_name, schema) if @search_params[aggregation_name].present?
          must_queries.push(aggregation_search_query) if aggregation_search_query
        end
      end
      if must_queries.any?
        query[:query] = {
          bool: {
            must: must_queries
          }
        }
      end
    else
      query = {query: {match_all: {}}} unless @additional_query || @additional_must_queries
    end
    return {} unless query[:query].present?
    query
  end

  # Search query pour une facet
  # Type :
  # - term(s): Recherche exactes par rapport a une string ou un tableau
  # - match: Recherche partielle
  # - Range: Recherche entre 2 valeurs données
  def search_query_for_field(name, schema)
    return {} unless @search_params[name].present?
    case schema[:type]
    when "multi_match"
      return {
        "multi_match": {
          "query": @search_params[name],
          "type": "most_fields",
          "fields": schema[:field],
          "operator": "and"
        }
      }
    when "term"
      search = @search_params[name]
      if search.is_a?(Array)
        search = search - %w(none)
      else
        return {} if search == "none"
      end
      query = {
        "term#{'s' if @search_params[name].is_a?(Array) }": {
          "#{schema[:field]}": search
        }
      }
      if @search_params[name].include?("#none")
        query = {
          bool: {
            should: [
              {
                "bool": { must_not: { exists: { field: 'follower_ids' } } }
              },
              query
            ]
          }
        }
      end
      return query
    when "terms"
      if schema[:operator] && schema[:operator] == "or"
        return {
          "bool": {
            "should": @search_params[name].map do |search_param|
              {
                "term": {
                  "#{schema[:field]}": search_param
                }
              }
            end
          }
        }
      end
      return @search_params[name].map{ |v| { "term": { "#{schema[:field]}": v } } }
    when "match"
      return {
        "match": {
          "#{schema[:field]}": @search_params[name]
        }
      }
    when "range"
      value_query = {}
      if @search_params["#{name}"]
        value_query[:lte] = @search_params["#{name}"]["lte"].to_i if @search_params["#{name}"]["lte"].present?
        value_query[:gte] = @search_params["#{name}"]["gte"].to_i if @search_params["#{name}"]["gte"].present?
      end
      if value_query.present?
        return {
          range: {
            "#{schema[:field]}": value_query
          }
        }
      else
        {}
      end
    else {}
    end
  end

  def aggregation_query_for_schema
    query = {aggs: {}}
    @schema.each do |aggregation_name, schema|
      if schema[:facet]
        query[:aggs].deep_merge!(aggregation_query_for_field(aggregation_name, schema))
      end
    end
    query
  end

  def aggregation_query_for_field(aggregation_name, schema)
    {
      "#{aggregation_name}": {
        terms: {
          field: schema[:field],
          size: 9999
        }
      }
    }
  end


end
