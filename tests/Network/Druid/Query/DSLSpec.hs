--
-- Copyright © 2013-2015 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RebindableSyntax #-}
{-# LANGUAGE TypeOperators #-}

module Network.Druid.Query.DSLSpec where

import Test.Hspec
import Control.Applicative
import Data.Aeson
import Network.Druid.Query.DSL
import Data.Time.QQ
import Network.Druid.Query.AST
import Control.Monad.Indexed.Free
import Control.Monad.Indexed
import Data.String
import Network.Druid.Query.ASTSpec(groupByQueryQ)
import Prelude hiding (Monad(..))

-- * Data source
data SampleDS = SampleDS

-- * Metrics
data CarrierM = CarrierM
data MakeM = MakeM
data DeviceM = DeviceM
data UserCountM = UserCountM
data DataTransferM = DataTransferM

-- * Vars
data TotalUsageV = TotalUsageV
data DataTransferV = DataTransferV

-- * Mapping to schema
instance DataSource SampleDS where
    dataSourceName _ = "sample_datasource"
instance Dimension CarrierM where
    dimensionName _ = "carrier"
instance Dimension DeviceM where
    dimensionName _ = "device"
instance Dimension MakeM where
    dimensionName _ = "make"
instance Metric UserCountM where
    metricName _ = "user_count"
instance Metric DataTransferM where
    metricName _ = "data_transfer"

instance MetricVar TotalUsageV where
    metricVarName _ = "total_usage"
instance MetricVar DataTransferV where
    metricVarName _ = "data_transfer"

-- * Relationships
instance HasDimension SampleDS CarrierM
instance HasDimension SampleDS MakeM
instance HasDimension SampleDS DeviceM
instance HasMetric SampleDS UserCountM
instance HasMetric SampleDS DataTransferM

(>>=) :: IxMonad m => m i j a -> (a -> m j k b) -> m i k b
(>>=) = (>>>=)

(>>) :: IxMonad m => m i j a -> m j k b -> m i k b
a >> b = a >>>= \_ -> b

fail :: String -> a
fail = error
return :: IxPointed m => a -> m i i a
return = ireturn

groupByQueryL :: QueryF SampleDS st (DoubleSum DataTransferM :| LongSum UserCountM :| st) ()
groupByQueryL = do
    applyFilter $ filterSelector CarrierM "AT&T"
    applyFilter $ filterOr [ filterSelector MakeM "Apple"
                           , filterSelector MakeM "Samsung"
                           ]
    longSum UserCountM
    doubleSum DataTransferM

    x <- deref (LongSum UserCountM)
    postAggregate $ x |+| x |+| x
    return ()

spec :: Spec
spec = 
    describe "ideal DSL" $ do
        it "builds correct query" $ do
            let q = groupByQuery SampleDS
                                 [SomeDimension CarrierM, SomeDimension DeviceM]
                                 GranularityDay
                                 [ Interval [utcIso8601| 2012-01-01 |]
                                            [utcIso8601| 2012-01-03 |] ]
                                 groupByQueryL
            encode q `shouldBe` encode groupByQueryQ
