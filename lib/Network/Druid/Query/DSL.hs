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
import Data.Scientific
import Data.Monoid
import qualified Data.HashMap.Strict as HM
import Control.Monad.State.Strict
import Pipes
import Pipes.HTTP
import qualified Pipes.Aeson.Unchecked as P
import Data.ByteString(ByteString)
import Lens.Family((^.))


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
  deriving Functor

newtype FilterL ds = FilterL { unFilterL :: Filter }
  deriving ToJSON

type QueryF ds = Free (QueryL ds)

newtype BoundMetric ds var = BoundMetric Text

data SomeDimension ds = forall a. HasDimension ds a => SomeDimension a

data FlattenedQuery = FlattenedQuery {
    _flattenedQueryAggregations :: [Aggregation]
}
  deriving Show

instance Monoid FlattenedQuery where
    mempty = FlattenedQuery mempty
    (FlattenedQuery a) `mappend` (FlattenedQuery b) =
        FlattenedQuery (a <> b)

-- * Decoding

class DruidDecode src dst where

-- | Fundep is just to help type inference.
class CanLookup ds key val | key -> val where
    doLookup :: ds -> key -> Object -> val

instance CanLookup ds (BoundMetric ds var) Scientific where
    doLookup _ (BoundMetric name) hm = do
        let e = HM.lookup "event" hm
        case e of
            Nothing -> error "no key 'event'"
            Just (Object hm') ->
                case HM.lookup name hm' of
                    Nothing -> error $ "no key: " <> T.unpack name
                    Just (Number n) -> n
                    Just x -> error $ "Not a number: " <> show x
            Just x -> error $ "Was not a JSON Object: " <> show x
            

groupBy
    :: DataSource ds
    => String
    -> ds
    -> [SomeDimension ds]
    -> Granularity
    -> [Interval]
    -> QueryF ds a
    -> (a -> (CanLookup ds key val => key -> Object -> val) -> Producer Object IO () -> IO ())
    -> IO ()
groupBy uri ds dimensions granularity intervals qf user_f = do
    let query = groupByQuery ds dimensions granularity intervals qf
    val <- go qf
    req <- parseUrl uri
    let req' = req { method = "POST"
                   , requestBody = RequestBodyLBS (encode query)
                   }

    withManager tlsManagerSettings $ \m ->
        withHTTP req' m $ \resp -> do
            user_f val (lookupVal ds) (void $ responseBody resp ^. P.decoded)
            -- let prod = (view (P.decoded undefined) wat) :: Producer Value IO ()
            -- thing <- (evalStateT (void P.decode) wat) :: Producer Value IO ()
            --user_f val (lookupVal ds) thing
  where
    lookupVal :: CanLookup ds key val => ds -> key -> Object ->  val
    lookupVal = doLookup 

    go (Pure r) = return r
    -- Just traverse to the end
    go (Free x) = case x of
        QueryLAggregation _ k -> go k
        QueryLFilter      _ k -> go k

groupByQuery
    :: DataSource ds
    => ds
    -> [SomeDimension ds]
    -> Granularity
    -> [Interval]
    -> QueryF ds a
    -> Query
groupByQuery ds dimensions granularity intervals qf = QueryGroupBy
    { _queryDataSourceName = DataSourceName $ dataSourceName ds
    , _queryDimensionNames =
        fmap (\(SomeDimension d) -> DimensionName $ dimensionName d) dimensions
    , _queryLimitSpec = Nothing
    , _queryHaving = Nothing
    , _queryGranularity = granularity
    , _queryAggregations = _flattenedQueryAggregations
    , _queryPostAggregations = Nothing
    , _queryFilter = Nothing
    , _queryIntervals = intervals
    }
  where
    FlattenedQuery{..} = flattenQuery qf

flattenQuery :: QueryF ds a -> FlattenedQuery
flattenQuery = mconcat . go
  where
    go :: QueryF ds a -> [FlattenedQuery]
    go (Pure _) = []
    go (Free x) = case x of
        QueryLFilter _ k ->
            go k
        QueryLAggregation agg k ->
            mempty { _flattenedQueryAggregations = [agg] } : go k

applyFilter :: FilterL ds -> QueryF ds ()
applyFilter filt = liftF $ QueryLFilter filt ()

filterAnd :: [FilterL ds] -> FilterL ds
filterAnd = FilterL . FilterAnd . fmap unFilterL

filterSelector :: HasDimension ds d => d -> Text -> FilterL ds
filterSelector dimension =
    FilterL . FilterSelector (DimensionName $ dimensionName dimension)

-- * Aggregators


longSum :: (HasMetric ds m, MetricVar v) => m -> v -> QueryF ds (BoundMetric ds v)
longSum metric var = liftF $ QueryLAggregation
    (AggregationLongSum (OutputName $ metricVarName var)
                        (MetricName $ metricName metric))
    (BoundMetric $ metricVarName var)

count :: MetricVar v => v -> QueryF ds (BoundMetric ds v)
count var = liftF $ QueryLAggregation
    (AggregationCount (OutputName $ metricVarName var))
    (BoundMetric $ metricVarName var)
