--
-- Copyright © 2013-2015 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GADTSyntax #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}

module Network.Druid.Query.DSL
(
    -- * Specifying data sources, metrics and dimensions.
    --
    -- | XXX Some words about data source specification
    DataSource(..),
    Dimension(..),
    HasDimension,
    SomeDimension(..),
    Metric(..),
    HasMetric,

    -- * DSL types
    QueryL,
    QueryF,
    AggregationL,
    FilterL,
    PostAggregationL,

    -- * DSL syntax
    letF,
    emitF,
    filterF,
    postAggregationF,

    -- * Filtering
    filterAnd,
    filterOr,
    filterSelector,

    -- * Aggregators
    longSum,
    doubleSum,
    count,

    -- * Post aggregators
    (|+|),
    (|/|),
    (|*|),
    (|-|),
    quotient,

    -- * Querying
    groupByQuery,

    -- * AST re-exports
    Granularity(..),
    Interval(..),
)
where

import Network.Druid.Query.AST

import Data.Text(Text)
import Control.Monad.Free
import Data.Aeson
import qualified Data.Text as T
import Data.Monoid
import Control.Applicative

-- | A 'DataSource' in druid is equivalent to a database "table".
class DataSource ds where
    dataSourceName :: ds -> Text

-- | 'DataSource's have any number of 'Dimensions'
class Dimension d where
    dimensionName :: d -> Text

-- | A relation from 'DataSource' to 'Dimension' 
class Dimension d => HasDimension ds d
 
-- | 'DataSource's have any number of 'Metric's
class Metric m where
    metricName :: m -> Text

-- | A relation from 'DataSource' to 'Metric' 
class Metric m => HasMetric ds m

-- | A query language
data QueryL ds a where
    -- Lifting a filter into the DSL. Multiple filters are anded together.
    QueryLFilter
        :: FilterL ds
        -> a
        -> QueryL ds a
    -- Emit an 'Aggregation' as a named output
    QueryLAggregationEmit
        :: AggregationL ds
        -> OutputName
        -> a
        -> QueryL ds a
    -- Bind an 'Aggregation' as 'PostAggregation' field lookup via HOAS.
    QueryLAggregationLet
        :: AggregationL ds
        -> (PostAggregationL ds
        -> QueryF ds a)
        -> QueryL ds a
    -- Lift a single post aggregation
    QueryLPostAggregation
        :: PostAggregationL ds
        -> OutputName
        -> a
        -> QueryL ds a
  deriving Functor

-- | The 'Aggregation' sub-language. Values at this level are things like
-- 'doubleSum'. These must be lifted into the 'QueryF' free monad via 'letF' or
-- 'emitF'.
data AggregationL ds = AggregationL
    { _createAggregation :: OutputName -> Aggregation }

-- | The 'Filter' sub-language. Values at this level are things like
-- 'filterSelector'. These must be lifted into the 'QueryF' free monad via
-- 'filterF'.
newtype FilterL ds = FilterL { unFilterL :: Filter }

-- | The 'PostAggregation' sub-language. Values at this level are things like
-- '(|*|)'. These must be lifted into the 'QueryF' free monad via
-- 'postAggregationF'. You can bind 'Aggregators' to 'PostAggregation's via
-- 'letF'.
newtype PostAggregationL ds = PostAggregationL PostAggregation

-- | The QueryF free monad
type QueryF ds = Free (QueryL ds)

-- | An existential wrapper for wrapping things that are a 'Dimenson' of a
-- given 'DataSource'. Used for functions like 'groupByQueryL'.
data SomeDimension ds = forall a. HasDimension ds a => SomeDimension a

-- | A data type to hold the accumulations whilst traversing our QueryF
data FlattenedQuery = FlattenedQuery
    { _flattenedQueryAggregations     :: [Aggregation]
    , _flattenedQueryPostAggregations :: Maybe [PostAggregation]
    , _flattenedQueryFilter           :: Maybe Filter
    }
  deriving Show

instance Monoid FlattenedQuery where
    mempty = FlattenedQuery mempty mempty Nothing
    mappend (FlattenedQuery agg1 pagg1 filt1)
            (FlattenedQuery agg2 pagg2 filt2) =
        FlattenedQuery (agg1 <> agg2)
                       (pagg1 <> pagg2)
                       (joinFilters filt1 filt2)

joinFilters :: Maybe Filter -> Maybe Filter -> Maybe Filter
joinFilters (Just filt1) (Just filt2)
    = Just $ FilterAnd [filt1, filt2]
joinFilters filt1 filt2
    = filt1 <|> filt2

letF :: AggregationL ds -> (PostAggregationL ds -> QueryF ds a) -> QueryF ds a
letF agg k = liftF $ QueryLAggregationLet agg k

emitF :: OutputName -> AggregationL ds -> QueryF ds ()
emitF on agg = liftF $ QueryLAggregationEmit agg on ()

filterF :: FilterL ds -> QueryF ds ()
filterF filt = liftF $ QueryLFilter filt ()

postAggregationF :: OutputName -> PostAggregationL ds -> QueryF ds ()
postAggregationF on pa = liftF $ QueryLPostAggregation pa on ()

groupByQuery
    :: DataSource ds
    => ds
    -> [SomeDimension ds]
    -> Granularity
    -> [Interval]
    -> QueryF ds a
    -> Query
groupByQuery ds dimensions granularity intervals qf = QueryGroupBy
    { _queryDataSourceName   = DataSourceName $ dataSourceName ds
    , _queryDimensionNames   =
        fmap (\(SomeDimension d) -> DimensionName $ dimensionName d) dimensions
    , _queryLimitSpec        = Nothing
    , _queryHaving           = Nothing
    , _queryGranularity      = granularity
    , _queryAggregations     = _flattenedQueryAggregations
    , _queryPostAggregations = _flattenedQueryPostAggregations
    , _queryFilter           = _flattenedQueryFilter
    , _queryIntervals        = intervals
    }
  where
    FlattenedQuery{..} = flattenQuery qf

flattenQuery :: QueryF ds a -> FlattenedQuery
flattenQuery = mconcat . go 0 -- 0 is where variable naming starts
  where
    go :: Int -> QueryF ds a -> [FlattenedQuery]
    go _ (Pure _) = []
    go i (Free x) = case x of
        QueryLFilter (FilterL filt) k ->
            mempty { _flattenedQueryFilter = Just filt } : go i k
        QueryLAggregationEmit (AggregationL agg) on k ->
            mempty { _flattenedQueryAggregations = [agg on] } : go i k
        QueryLAggregationLet (AggregationL agg) k ->
            let on = OutputName $ "__var_" <> T.pack (show i)
                k' = k $ PostAggregationL $ PostAggregationFieldAccess on
            in mempty { _flattenedQueryAggregations = [agg on] } : go (succ i) k'
        QueryLPostAggregation (PostAggregationL pagg) on k ->
            -- Replace the last post aggregation's automatically generated
            -- output name with the user-provided one
            let pagg' = pagg { _postAggregationName = on }
            in mempty { _flattenedQueryPostAggregations = Just [pagg'] } : go i k

filterAnd :: [FilterL ds] -> FilterL ds
filterAnd = FilterL . FilterAnd . fmap unFilterL

filterOr :: [FilterL ds] -> FilterL ds
filterOr = FilterL . FilterOr . fmap unFilterL

filterSelector :: HasDimension ds d => d -> Text -> FilterL ds
filterSelector dimension =
    FilterL . FilterSelector (DimensionName $ dimensionName dimension)

longSum :: HasMetric ds m => m -> AggregationL ds
longSum metric = AggregationL $ \v -> 
    AggregationLongSum v (MetricName $ metricName metric)

doubleSum :: HasMetric ds m => m -> AggregationL ds
doubleSum metric = AggregationL $ \v -> 
    AggregationDoubleSum v (MetricName $ metricName metric)

count :: AggregationL ds
count = AggregationL AggregationCount


(|+|) :: PostAggregationL ds -> PostAggregationL ds -> PostAggregationL ds
(|+|) = arithHelper "sum_" APlus

(|/|) :: PostAggregationL ds -> PostAggregationL ds -> PostAggregationL ds
(|/|) = arithHelper "div_" ADiv

(|*|) :: PostAggregationL ds -> PostAggregationL ds -> PostAggregationL ds
(|*|) = arithHelper "mult_" AMult

(|-|) :: PostAggregationL ds -> PostAggregationL ds -> PostAggregationL ds
(|-|) = arithHelper "minus_" AMinus

quotient :: PostAggregationL ds -> PostAggregationL ds -> PostAggregationL ds
quotient = arithHelper "quot_" AQuot

arithHelper
    :: Text
    -> ArithmeticFunction
    -> PostAggregationL ds
    -> PostAggregationL ds
    -> PostAggregationL ds
arithHelper var arith (PostAggregationL pa1) (PostAggregationL pa2) = PostAggregationL $
    let OutputName on1 = _postAggregationName pa1
        OutputName on2 = _postAggregationName pa2
    in PostAggregationArithmetic (OutputName $ var <> on1 <> "_" <> on2) arith [pa1, pa2] Nothing
