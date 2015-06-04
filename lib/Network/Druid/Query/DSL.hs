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
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}


module Network.Druid.Query.DSL
where

import Network.Druid.Query.AST

import Data.Text(Text)
import Data.Aeson
import GHC.Exts(Constraint)
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as LBS
import Data.Monoid
import qualified Data.HashMap.Strict as HM
import Control.Applicative
import Control.Monad.State.Strict
import Data.Functor.Indexed
import Data.Scientific
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Data.ByteString(ByteString)
import Control.Monad.Indexed.Free

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

data QueryL ds st st' a where
    QueryLFilter :: FilterL ds -> a -> QueryL ds st st' a
    QueryLAggregation :: Aggregation -> a -> QueryL ds st st' a
    QueryLPostAggregation :: PostAggregationL ds -> a -> QueryL ds st st' a

instance IxFunctor (QueryL ds) where
    imap f (QueryLFilter filt a) = QueryLFilter filt (f a)
    imap f (QueryLAggregation agg a) = QueryLAggregation agg (f a)
    imap f (QueryLPostAggregation agg a) = QueryLPostAggregation agg (f a)


newtype FilterL ds
    = FilterL { unFilterL :: Filter }
  deriving ToJSON

data PostAggregationL ds = PostAggregationL Text PostAggregation

type QueryF ds = IxFree (QueryL ds)

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

data a :| b
infixr 9 :| 

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
    -> QueryF ds st st' a
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

flattenQuery :: QueryF ds st st' a -> FlattenedQuery
flattenQuery = mconcat . go
  where
    go :: QueryF ds st st' a -> [FlattenedQuery]
    go (Pure _) = []
    go (Free x) = case x of
        QueryLFilter (FilterL filt) k ->
            mempty { _flattenedQueryFilter = Just filt } : go k
        QueryLAggregation agg k ->
            mempty { _flattenedQueryAggregations = [agg] } : go k
        QueryLPostAggregation (PostAggregationL _ pagg) k ->
            mempty { _flattenedQueryPostAggregations = Just [pagg] } : go k

applyFilter :: FilterL ds -> QueryF ds st st ()
applyFilter filt = iliftFree $ QueryLFilter filt ()

filterAnd :: [FilterL ds] -> FilterL ds
filterAnd = FilterL . FilterAnd . fmap unFilterL

filterOr :: [FilterL ds] -> FilterL ds
filterOr = FilterL . FilterOr . fmap unFilterL

filterSelector :: HasDimension ds d => d -> Text -> FilterL ds
filterSelector dimension =
    FilterL . FilterSelector (DimensionName $ dimensionName dimension)

-- * Aggregators

data LongSum m = LongSum m

longSum :: HasMetric ds m => m -> QueryF ds st (LongSum m :| st) ()
longSum metric = iliftFree $ QueryLAggregation
    (AggregationLongSum (OutputName $ boundName (LongSum metric))
                        (MetricName $ metricName metric))
    ()

data DoubleSum m = DoubleSum m

doubleSum :: (HasMetric ds m) => m -> QueryF ds st (DoubleSum m :| st) ()
doubleSum metric = iliftFree $ QueryLAggregation
    (AggregationDoubleSum (OutputName $ boundName (DoubleSum metric))
                          (MetricName $ metricName metric))
    ()

data PostAggVar

type family IsBound x xs :: Constraint where
    IsBound x (x :| xs) = ()
    IsBound x (y :| xs) = IsBound x xs

class Metric m => Bindable agg m where
    boundName :: agg m -> Text

instance Metric m => Bindable DoubleSum m where
    boundName (DoubleSum m) = "double_sum_" <> metricName m

instance Metric m => Bindable LongSum m where
    boundName (LongSum m) = "long_sum_" <> metricName m

deref :: (Bindable agg m, IsBound (agg m) st) => agg m -> QueryF ds st st (PostAggregationL ds)
deref agg = return $
    PostAggregationL (boundName agg)
                     (PostAggregationFieldAccess $ OutputName $ boundName agg)

postAggregate :: PostAggregationL ds -> QueryF ds st st ()
postAggregate pagg = iliftFree $ QueryLPostAggregation pagg ()

(|+|) :: PostAggregationL ds -> PostAggregationL ds -> PostAggregationL ds
(PostAggregationL n1 pa1) |+| (PostAggregationL n2 pa2) =
    let on = "pa_sum_" <> n1 <> "_plus_" <> n2
    in PostAggregationL on $ PostAggregationArithmetic (OutputName on)
                                                       APlus
                                                       [pa1, pa2]
                                                       Nothing


count :: MetricVar v => v -> QueryF ds st st' (BoundMetric ds v)
count var = iliftFree $ QueryLAggregation
    (AggregationCount (OutputName $ metricVarName var))
    (BoundMetric $ metricVarName var)
