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
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}


module Network.Druid.Query.DSL
where

import Network.Druid.Query.AST

import Data.Text(Text)
import Control.Monad.Free
import Data.Aeson
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as LBS
import Data.Monoid
import qualified Data.HashMap.Strict as HM
import Control.Applicative
import Control.Monad.State.Strict
import Data.Scientific
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Data.ByteString(ByteString)

class DataSource ds where
    dataSourceName :: ds -> Text

class Dimension d where
    dimensionName :: d -> Text

class Dimension d => HasDimension ds d

class Metric m where
    metricName :: m -> Text

class Metric m => HasMetric ds m

class MetricVar v where
    metricVarName :: v -> Text

data QueryL ds a where
    QueryLFilter :: FilterL ds -> a -> QueryL ds a
    QueryLAggregation :: Aggregation -> a -> QueryL ds a
    QueryLPostAggregation :: PostAggregation -> a -> QueryL ds a
  deriving Functor

newtype FilterL ds = FilterL { unFilterL :: Filter }
  deriving ToJSON

type QueryF ds = Free (QueryL ds)

newtype BoundMetric ds var = BoundMetric Text

data SomeDimension ds = forall a. HasDimension ds a => SomeDimension a

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

runQuery :: String -> Query -> IO (Either String Value)
runQuery uri query = do
    req <- parseUrl uri
    let req' = req { method = "POST"
                   , requestBody = RequestBodyLBS (encode query)
                   }

    withManager tlsManagerSettings $ \m ->
        withResponse req' m $ \resp -> do
            -- Do not stream, Druid will not stream, plus we're dealing with
            -- JSON so let's just not go there.
            eitherDecode . LBS.fromChunks <$> brConsume (responseBody resp)

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
flattenQuery = mconcat . go
  where
    go :: QueryF ds a -> [FlattenedQuery]
    go (Pure _) = []
    go (Free x) = case x of
        QueryLFilter (FilterL filt) k ->
            mempty { _flattenedQueryFilter = Just filt } : go k
        QueryLAggregation agg k ->
            mempty { _flattenedQueryAggregations = [agg] } : go k
        QueryLPostAggregation pagg k ->
            mempty { _flattenedQueryPostAggregations = Just [pagg] } : go k

applyFilter :: FilterL ds -> QueryF ds ()
applyFilter filt = liftF $ QueryLFilter filt ()

filterAnd :: [FilterL ds] -> FilterL ds
filterAnd = FilterL . FilterAnd . fmap unFilterL

filterOr :: [FilterL ds] -> FilterL ds
filterOr = FilterL . FilterOr . fmap unFilterL

filterSelector :: HasDimension ds d => d -> Text -> FilterL ds
filterSelector dimension =
    FilterL . FilterSelector (DimensionName $ dimensionName dimension)

-- * Aggregators

longSum :: (MetricVar v, HasMetric ds m) => v -> m -> QueryF ds (BoundMetric ds v)
longSum var metric = liftF $ QueryLAggregation
    (AggregationLongSum (OutputName $ metricVarName var)
                        (MetricName $ metricName metric))
    (BoundMetric $ metricVarName var)

doubleSum :: (MetricVar v, HasMetric ds m) => v -> m -> QueryF ds (BoundMetric ds v)
doubleSum var metric = liftF $ QueryLAggregation
    (AggregationDoubleSum (OutputName $ metricVarName var)
                          (MetricName $ metricName metric))
    (BoundMetric $ metricVarName var)


count :: MetricVar v => v -> QueryF ds (BoundMetric ds v)
count var = liftF $ QueryLAggregation
    (AggregationCount (OutputName $ metricVarName var))
    (BoundMetric $ metricVarName var)
