--
-- Copyright © 2013-2015 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RecordWildCards            #-}

module Network.Druid.Query
(
    -- * Query ADT
    Query(..),
    Threshold(..),
    DataSource(..),
    Granularity(..),
    Filter(..),
    Dimension(..),
    Aggregation(..),
    PostAggregation(..),
    Interval(..),
    Metric(..),

) where

import Control.Applicative
import Data.Aeson
import Data.Maybe
import Data.Monoid
import Data.String
import Data.Text

-- | Druid has numerous query types for various use cases. Queries are composed
-- of various JSON properties and Druid has different types of queries for
-- different use cases.
data Query
    -- | These types of queries take a timeseries query object and return an
    -- array of JSON objects where each object represents a value asked for by
    -- the timeseries query.
    = QueryTimeSeries
        { _queryDataSource       :: DataSource
        , _queryGranularity      :: Granularity
        , _queryFilter           :: Maybe Filter
        , _queryAggregations     :: [Aggregation]
        , _queryPostAggregations :: [PostAggregation]
        , _queryIntervals        :: [Interval]
        }
    -- | TopN queries return a sorted set of results for the values in a given
    -- dimension according to some criteria. Conceptually, they can be thought
    -- of as an approximate GroupByQuery over a single dimension with an
    -- Ordering spec. TopNs are much faster and resource efficient than
    -- GroupBys for this use case. These types of queries take a topN query
    -- object and return an array of JSON objects where each object represents
    -- a value asked for by the topN query.
    --
    -- TopNs are approximate in that each node will rank their top K results
    -- and only return those top K results to the broker. K, by default in
    -- Druid, is max(1000, threshold). In practice, this means that if you ask
    -- for the top 1000 items ordered, the correctness of the first ~900 items
    -- will be 100%, and the ordering of the results after that is not
    -- guaranteed. TopNs can be made more accurate by increasing the threshold.
    | QueryTopN
        { _queryDataSource       :: DataSource
        , _queryGranularity      :: Granularity
        , _queryFilter           :: Maybe Filter
        , _queryAggregations     :: [Aggregation]
        , _queryPostAggregations :: [PostAggregation]
        , _queryIntervals        :: [Interval]
        , _queryDimensions       :: [Dimension]
        , _queryThreshold        :: Threshold
        , _queryMetric           :: Metric
        }
    -- | These types of queries take a groupBy query object and return an array
    -- of JSON objects where each object represents a grouping asked for by the
    -- query. Note: If you only want to do straight aggregates for some time
    -- range, we highly recommend using TimeseriesQueries instead. The
    -- performance will be substantially better. If you want to do an ordered
    -- groupBy over a single dimension, please look at TopN queries. The
    -- performance for that use case is also substantially better.
    | QueryGroupBy

data Threshold

-- | A data source is the Druid equivalent of a database table. However, a
-- query can also masquerade as a data source, providing subquery-like
-- functionality. Query data sources are currently supported only by GroupBy
-- queries.
data DataSource =
    DataSourceString { _dataSourceString :: Text }


-- | The granularity field determines how data gets bucketed across the time
-- dimension, or how it gets aggregated by hour, day, minute, etc.
--
-- It can be specified either as a string for simple granularities or as an
-- object for arbitrary granularities.
data Granularity
    = GranularityAll
    | GranularityNone
    | GranularityMinute
    | GranularityFifteenMinute
    | GranularityThirtyMinute
    | GranularityHour
    | GranularityDay

-- | A filter is a JSON object indicating which rows of data should be included
-- in the computation for a query. It’s essentially the equivalent of the WHERE
-- clause in SQL. Druid supports the following types of filters.
data Filter
    -- | The simplest filter is a selector filter. The selector filter will
    -- match a specific dimension with a specific value. Selector filters can
    -- be used as the base filters for more complex Boolean expressions of
    -- filters.
    = FilterSelector
        { _selectorDimension :: Dimension
        , _selectorValue     :: Text
        }
    | FilterRegularExpression
        { _selectorDimension :: Dimension
        , _selectorPattern   :: Text }
    | FilterJS
        { _selectorDimension :: Dimension
        , _selectorFunction  :: JS }
    | FilterAnd { _selectorFields :: [Filter] }
    | FilterOr  { _selectorFields :: [Filter] }
    | FilterNot { _selectorField :: Filter }

-- | TODO: Undocumented
newtype Dimension = Dimension { unDimension :: Text }
    deriving (IsString, ToJSON)

newtype JS = JS { unJS :: Text }
    deriving (IsString, ToJSON)

-- | TODO: Undocumented
newtype OutputName = OutputName { unOutputName :: Text }
    deriving (IsString, ToJSON)

-- | TODO: Undocumented
newtype MetricName = MetricName { unMetricName :: Text }
    deriving (IsString, ToJSON)

-- | Aggregations are specifications of processing over metrics available in
-- Druid. Available aggregations are:
data Aggregation
    = AggregationCount { _aggregationName :: OutputName }
    | AggregationLongSum
        { _aggregationName      :: OutputName
        , _aggregationFieldName :: MetricName }
    | AggregationDoubleSum
        { _aggregationName      :: OutputName
        , _aggregationFieldName :: MetricName }
    | AggregationMin
        { _aggregationName      :: OutputName
        , _aggregationFieldName :: MetricName }
    | AggregationMax
        { _aggregationName      :: OutputName
        , _aggregationFieldName :: MetricName }
    | AggregationHyperUnique
        { _aggregationName      :: OutputName
        , _aggregationFieldName :: MetricName }
    | AggregationJS
        { _aggregationName              :: OutputName
        , _aggregationFieldNames        :: [MetricName]
        , _aggregationAggregateFunction :: JS
        , _aggregationCombineFunction   :: JS
        , _aggregationResetFunction     :: JS
        }
    | AggregationCardinality
        { _aggregationName       :: OutputName
        , _aggregationFieldNames :: [MetricName]
        , _aggregationByRow      :: Bool
        }
    | AggregationFiltered
        { _aggregationFilter     :: Filter
        , _aggregationAggregator :: Aggregation
        }

data PostAggregation
data Interval
data Metric

-- * Instances

instance ToJSON Query where
    toJSON QueryTimeSeries{..} = object $
        [ "queryType" .= String "timeseries"
        , "granularity" .= toJSON _queryGranularity
        , "dataSource"  .= toJSON _queryDataSource
        , "aggregations" .= toJSON _queryAggregations
        ]
        <> fmap (("filter" .=) . toJSON) (maybeToList _queryFilter)

instance ToJSON Aggregation where
    toJSON _ = error "no aggregation for you"

instance ToJSON DataSource where
    toJSON DataSourceString{..} = String _dataSourceString

instance ToJSON Granularity where
    toJSON GranularityAll           = "all"
    toJSON GranularityNone          = "none"
    toJSON GranularityMinute        = "minute"
    toJSON GranularityFifteenMinute = "fifteen_minute"
    toJSON GranularityThirtyMinute  = "thirty_minute"
    toJSON GranularityHour          = "hour"
    toJSON GranularityDay           = "day"

instance ToJSON Filter where
    toJSON FilterSelector{..} = object $
        [ "type" .= String "selector"
        , "dimension" .= _selectorDimension
        , "value" .= _selectorValue
        ]
    toJSON FilterRegularExpression{..} = object $
        [ "type" .= String "regex"
        , "dimension" .= _selectorDimension
        , "pattern" .= _selectorPattern
        ]
    toJSON FilterJS{..} = object $
        [ "type" .= String "javascript"
        , "dimension" .= _selectorDimension
        , "function" .=  _selectorFunction
        ]
    toJSON FilterAnd{..} = object $
        [ "type" .= String "and"
        , "fields" .=  toJSON _selectorFields
        ]
    toJSON FilterOr{..} = object $
        [ "type" .= String "or"
        , "fields" .=  toJSON _selectorFields
        ]
    toJSON FilterNot{..} = object $
        [ "type" .= String "not"
        , "field" .=  toJSON _selectorField
        ]
