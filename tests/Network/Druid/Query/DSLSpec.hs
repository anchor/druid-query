--
-- Copyright © 2013-2015 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE QuasiQuotes           #-}

module Network.Druid.Query.DSLSpec where

import Network.Druid.Query.DSL

import Data.Aeson
import Data.Aeson.QQ
import Data.Time.QQ
import Test.Hspec

-- * Data source
data SampleDS = SampleDS

-- * Dimensions
data CarrierD = CarrierD
data MakeD = MakeD
data DeviceD = DeviceD

-- * Metrics
data UserCountM = UserCountM
data DataTransferM = DataTransferM

-- * Mapping to schema
instance DataSource SampleDS where
    dataSourceName _ = "sample_datasource"

instance Dimension CarrierD where
    dimensionName _ = "carrier"
instance Dimension DeviceD where
    dimensionName _ = "device"
instance Dimension MakeD where
    dimensionName _ = "make"

instance Metric UserCountM where
    metricName _ = "user_count"
instance Metric DataTransferM where
    metricName _ = "data_transfer"

-- * Relationships
instance HasDimension SampleDS CarrierD
instance HasDimension SampleDS MakeD
instance HasDimension SampleDS DeviceD

instance HasMetric SampleDS UserCountM
instance HasMetric SampleDS DataTransferM

groupByQueryL :: QueryF SampleDS ()
groupByQueryL = do
    filterF $ filterSelector CarrierD "AT&T"
    filterF $ filterOr [ filterSelector MakeD "Apple"
                       , filterSelector MakeD "Samsung"
                       ]
    letF (doubleSum DataTransferM) $ \transfer ->
        letF (longSum UserCountM) $ \user ->
            postAggregationF "usage_transfer" $ user |*| transfer

groupByQueryV :: Value
groupByQueryV = [aesonQQ|
{
   "postAggregations" : [
      {
         "fields" : [
            {
               "fieldName" : "__var_1",
               "type" : "fieldAccess"
            },
            {
               "type" : "fieldAccess",
               "fieldName" : "__var_0"
            }
         ],
         "name" : "usage_transfer",
         "fn" : "*",
         "type" : "arithmetic"
      }
   ],
   "queryType" : "groupBy",
   "dataSource" : "sample_datasource",
   "intervals" : [
      "2012-01-01T00:00:00/2012-01-03T00:00:00"
   ],
   "granularity" : "day",
   "dimensions" : [
      "carrier",
      "device"
   ],
   "aggregations" : [
      {
         "name" : "__var_0",
         "fieldName" : "data_transfer",
         "type" : "doubleSum"
      },
      {
         "name" : "__var_1",
         "fieldName" : "user_count",
         "type" : "longSum"
      }
   ],
   "filter" : {
      "fields" : [
         {
            "value" : "AT&T",
            "type" : "selector",
            "dimension" : "carrier"
         },
         {
            "type" : "or",
            "fields" : [
               {
                  "dimension" : "make",
                  "type" : "selector",
                  "value" : "Apple"
               },
               {
                  "dimension" : "make",
                  "value" : "Samsung",
                  "type" : "selector"
               }
            ]
         }
      ],
      "type" : "and"
   }
}
|]

spec :: Spec
spec =
    describe "ideal DSL" $
        it "builds correct query" $ do
            let q = groupByQuery SampleDS
                                 [ SomeDimension CarrierD
                                 , SomeDimension DeviceD
                                 ]
                                 GranularityDay
                                 [ Interval [utcIso8601| 2012-01-01 |]
                                            [utcIso8601| 2012-01-03 |] ]
                                 groupByQueryL
            toJSON q `shouldBe` groupByQueryV
