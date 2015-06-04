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

module Network.Druid.Query.DSLSpec where

import Network.Druid.Query.DSL

import Test.Hspec
import Data.Time.QQ
import Data.Aeson

-- * Data source
data SampleDS = SampleDS

-- * Metrics
data CarrierM = CarrierM
data MakeM = MakeM
data DeviceM = DeviceM
data UserCountM = UserCountM
data DataTransferM = DataTransferM

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

-- * Relationships
instance HasDimension SampleDS CarrierM
instance HasDimension SampleDS MakeM
instance HasDimension SampleDS DeviceM
instance HasMetric SampleDS UserCountM
instance HasMetric SampleDS DataTransferM

groupByQueryL :: QueryF SampleDS ()
groupByQueryL = do
    filterF $ filterSelector CarrierM "AT&T"
    filterF $ filterOr [ filterSelector MakeM "Apple"
                       , filterSelector MakeM "Samsung"
                       ]
    letF (doubleSum DataTransferM) $ \transfer -> 
        letF (longSum UserCountM) $ \user ->
            postAggregationF "usage_transfer" $ user |*| transfer

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
            toJSON q `shouldBe` ""
