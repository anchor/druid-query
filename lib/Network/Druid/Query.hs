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
    NumericalValue(..),
    ArithmeticFunction(..),
    PostAggregationOrdering(..),
    Interval(..),
    Metric(..),
    UTCTime(..)
) where

import Control.Applicative
import Data.Aeson
import Data.Maybe
import Data.Bifunctor
import Data.Monoid
import Data.Time.Locale.Compat(defaultTimeLocale)
import Data.Scientific (Scientific (..))
import Data.String
import Data.Text (Text)
import Data.Time.Clock(UTCTime(..))
import qualified Data.Text as T
import Data.Time.Format(formatTime)
import Data.Time.ISO8601(formatISO8601Millis)

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
        , _queryPostAggregations :: Maybe [PostAggregation]
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
        , _queryPostAggregations :: Maybe [PostAggregation]
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
        , _aggregationFunctionAggregate :: JS
        , _aggregationFunctionCombine   :: JS
        , _aggregationFunctionReset     :: JS
        }
    | AggregationCardinality
        { _aggregationName       :: OutputName
        , _aggregationFieldNames :: [MetricName]
        , _aggregationByRow      :: Maybe Bool
        }
    | AggregationFiltered
        { _aggregationFilter     :: Filter
        , _aggregationAggregator :: Aggregation
        }

-- | Post-aggregations are specifications of processing that should happen on
-- aggregated values as they come out of Druid. If you include a post
-- aggregation as part of a query, make sure to include all aggregators the
-- post-aggregator requires.
data PostAggregation
    -- | The arithmetic post-aggregator applies the provided function to the
    -- given fields from left to right. The fields can be aggregators or other
    -- post aggregators.
    --
    -- Supported functions are 'APlus', 'AMinus', 'AMulti', 'ADiv', and
    -- 'AQuot'.
    --
    -- Note:
    --
    -- Division always returns 0 if dividing by 0, regardless of the numerator.
    -- quotient division behaves like regular floating point division
    -- Arithmetic post-aggregators may also specify an ordering, which defines
    -- the order of resulting values when sorting results (this can be useful
    -- for 'TopN' queries for instance):
    --
    -- If no ordering (or 'PostAggregationOrderingNull') is specified, the
    -- default floating point ordering is used.
    -- 'PostAggregationOrderingNumericFirst' ordering always returns finite
    -- values first, followed by NaN, and infinite values last.
    = PostAggregationArithmetic
        { _postAggregationName               :: OutputName
        , _postAggregationArithmeticFunction :: ArithmeticFunction
        , _postAggregationFields             :: [PostAggregation]
        , _postAggregationOrdering           :: Maybe PostAggregationOrdering
        }
    -- | This returns the value produced by the specified aggregator.
    --
    -- fieldName refers to the output name of the aggregator given in the
    -- aggregations portion of the query.
    | PostAggregationFieldAccess
        { _postAggregationName      :: OutputName
        , _postAggregationFieldName :: OutputName }
    -- | The constant post-aggregator always returns the specified value.
    | PostAggregationConstant
        { _postAggregationName  :: OutputName
        , _postAggregationValue :: NumericalValue }
    -- | Applies the provided JavaScript function to the given fields. Fields
    -- are passed as arguments to the JavaScript function in the given order.
    | PostAggregationJS
        { _postAggregationName       :: OutputName
        , _postAggregationFieldNames :: [OutputName]
        , _postAggregationFunction   :: JS
        }
    -- | The 'PostAggregationHyperUniqueCardinality' post aggregator is used to
    -- wrap a hyperUnique object such that it can be used in post aggregations.
    | PostAggregationHyperUniqueCardinality
        { _postAggregationFieldName :: OutputName }

newtype NumericalValue = NumericalValue { unNumericalValue :: Scientific }
    deriving Num

-- | An arithmetic function as supported by 'PostAggregation'
data ArithmeticFunction
    -- | Addition
    = APlus
    -- | Subtraction
    | AMinus
    -- | Multiplication
    | AMult
    -- | Division
    | ADiv
    -- | Quotient
    | AQuot

-- | If PostAggregationOrderingNull is specified, the default floating point
-- ordering is used. 'PostAggregationOrderingNumericFirst' ordering always
-- returns finite values first, followed by NaN, and infinite values last.
data PostAggregationOrdering
    = PostAggregationOrderingNull | PostAggregationOrderingNumericFirst

data Interval = Interval
    { _intervalStart :: UTCTime
    , _intervalEnd   :: UTCTime
    }

data Metric

-- * Instances

instance ToJSON Query where
    toJSON QueryTimeSeries{..} = object $
        [ "queryType"    .= String "timeseries"
        , "granularity"  .= toJSON _queryGranularity
        , "dataSource"   .= toJSON _queryDataSource
        , "aggregations" .= toJSON _queryAggregations
        , "intervals"    .= toJSON _queryIntervals
        ]
        <> fmap ("postAggregations" .=) (maybeToList _queryPostAggregations)
        <> fmap ("filter" .=) (maybeToList _queryFilter)

instance ToJSON Interval where
    toJSON Interval{..} =
        let (l,r) = (fmt _intervalStart, fmt _intervalEnd)
        in String $ l <> "/" <> r 
      where
        fmt = T.pack . formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%Q"

instance ToJSON Aggregation where
    toJSON AggregationCount{..} = object $
        [ "type" .= String "count"
        , "name" .= _aggregationName
        ]
    toJSON AggregationLongSum{..} = object $
        [ "type"      .= String "longSum"
        , "name"      .= _aggregationName
        , "fieldName" .= _aggregationFieldName
        ]
    toJSON AggregationDoubleSum{..} = object $
        [ "type"      .= String "doubleSum"
        , "name"      .= _aggregationName
        , "fieldName" .= _aggregationFieldName
        ]
    toJSON AggregationMin{..} = object $
        [ "type"      .= String "min"
        , "name"      .= _aggregationName
        ]
    toJSON AggregationMax{..} = object $
        [ "type"      .= String "max"
        , "name"      .= _aggregationName
        ]
    toJSON AggregationJS{..} = object $
        [ "type"        .= String "javascript"
        , "name"        .= _aggregationName
        , "fieldNames"  .= _aggregationFieldNames
        , "fnAggregate" .= _aggregationFunctionAggregate
        , "fnCombine"   .= _aggregationFunctionCombine
        , "fnReset"     .= _aggregationFunctionReset
        ]
    toJSON AggregationCardinality{..} = object $
        [ "type"       .= String "cardinality"
        , "name"       .= _aggregationName
        , "fieldNames" .= _aggregationFieldNames
        ]
        <> fmap ("byRow" .=) (maybeToList _aggregationByRow)
    toJSON AggregationHyperUnique{..} = object $
        [ "type"      .= String "hyperUnique"
        , "name"      .= _aggregationName
        , "fieldName" .= _aggregationFieldName
        ]
    toJSON AggregationFiltered{..} = object $
        [ "type"       .= String "filtered"
        , "filter"     .= _aggregationFilter
        , "aggregator" .= _aggregationAggregator
        ]

instance ToJSON PostAggregation where
    toJSON PostAggregationArithmetic{..} = object $
        [ "type"   .= String "arithmetic"
        , "name"   .= _postAggregationName
        , "fn"     .= _postAggregationArithmeticFunction
        , "fields" .= _postAggregationFields
        ]
        <> fmap ("ordering" .=) (maybeToList _postAggregationOrdering)
    toJSON PostAggregationFieldAccess{..} = object $
        [ "type"      .= String "fieldAccess"
        , "name"      .= _postAggregationName
        , "fieldName" .= _postAggregationFieldName
        ]
    toJSON PostAggregationConstant{..} = object $
        [ "type"  .= String "constant"
        , "name"  .= _postAggregationName
        , "value" .= Number (unNumericalValue _postAggregationValue)
        ]
    toJSON PostAggregationJS{..} = object $
        [ "type"       .= String "javascript"
        , "name"       .= _postAggregationName
        , "fieldNames" .= _postAggregationFieldNames
        , "function"   .= _postAggregationFunction
        ]
    toJSON PostAggregationHyperUniqueCardinality{..} = object $
        [ "type"      .= String "hyperUniqueCardinality"
        , "fieldName" .= _postAggregationFieldName
        ]

instance ToJSON PostAggregationOrdering where
    toJSON PostAggregationOrderingNull         = Null
    toJSON PostAggregationOrderingNumericFirst = "numericFirst"

instance ToJSON ArithmeticFunction where
    toJSON APlus  = "+"
    toJSON AMinus = "-"
    toJSON AMult  = "*"
    toJSON ADiv   = "/"
    toJSON AQuot  = "quotient"

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
        [ "type"      .= String "selector"
        , "dimension" .= _selectorDimension
        , "value"     .= _selectorValue
        ]
    toJSON FilterRegularExpression{..} = object $
        [ "type"      .= String "regex"
        , "dimension" .= _selectorDimension
        , "pattern"   .= _selectorPattern
        ]
    toJSON FilterJS{..} = object $
        [ "type"      .= String "javascript"
        , "dimension" .= _selectorDimension
        , "function"  .= _selectorFunction
        ]
    toJSON FilterAnd{..} = object $
        [ "type"   .= String "and"
        , "fields" .= _selectorFields
        ]
    toJSON FilterOr{..} = object $
        [ "type"   .= String "or"
        , "fields" .= _selectorFields
        ]
    toJSON FilterNot{..} = object $
        [ "type"  .= String "not"
        , "field" .= _selectorField
        ]
