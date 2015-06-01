--
-- Copyright © 2013-2015 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Main where

import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Data.Aeson
import Data.Aeson.QQ
import Data.Aeson.Diff
import Data.Time.QQ

import Network.Druid.Query

main :: IO ()
main = hspec suite

-- * Example queries

timeSeriesQueryV :: Value
timeSeriesQueryV = [aesonQQ|
{
   "granularity" : "day",
   "filter" : {
      "type" : "and",
      "fields" : [
         {
            "value" : "sample_value1",
            "type" : "selector",
            "dimension" : "sample_dimension1"
         },
         {
            "type" : "or",
            "fields" : [
               {
                  "value" : "sample_value2",
                  "type" : "selector",
                  "dimension" : "sample_dimension2"
               },
               {
                  "value" : "sample_value3",
                  "type" : "selector",
                  "dimension" : "sample_dimension3"
               }
            ]
         }
      ]
   },
   "dataSource" : "sample_datasource",
   "postAggregations" : [
      {
         "name" : "sample_divide",
         "type" : "arithmetic",
         "fields" : [
            {
               "name" : "sample_name1",
               "type" : "fieldAccess",
               "fieldName" : "sample_fieldName1"
            },
            {
               "name" : "sample_name2",
               "fieldName" : "sample_fieldName2",
               "type" : "fieldAccess"
            }
         ],
         "fn" : "/"
      }
   ],
   "aggregations" : [
      {
         "name" : "sample_name1",
         "type" : "longSum",
         "fieldName" : "sample_fieldName1"
      },
      {
         "name" : "sample_name2",
         "type" : "doubleSum",
         "fieldName" : "sample_fieldName2"
      }
   ],
   "intervals" : [
      "2012-01-01T00:00:00/2012-01-03T00:00:00"
   ],
   "queryType" : "timeseries"
}
|]

timeSeriesQueryQ :: Query
timeSeriesQueryQ = QueryTimeSeries
    { _queryDataSource = DataSourceString "sample_datasource"
    , _queryGranularity = GranularityDay
    , _queryFilter = Just $ FilterAnd
        [ FilterSelector "sample_dimension1" "sample_value1"
        , FilterOr
            [ FilterSelector "sample_dimension2" "sample_value2"
            , FilterSelector "sample_dimension3" "sample_value3"
            ]
        ]
    , _queryAggregations =
        [ AggregationLongSum "sample_name1" "sample_fieldName1"
        , AggregationDoubleSum "sample_name2" "sample_fieldName2"
        ]
    , _queryPostAggregations = Just
        [ PostAggregationArithmetic
            "sample_divide"
            ADiv
            [ PostAggregationFieldAccess "sample_name1" "sample_fieldName1"
            , PostAggregationFieldAccess "sample_name2" "sample_fieldName2"
            ]
            Nothing
        ]
    , _queryIntervals =
        [ Interval [utcIso8601| 2012-01-01 |] [utcIso8601| 2012-01-03 |] ]
    }

topNQueryV :: Value
topNQueryV = [aesonQQ|
{
  "queryType": "topN",
  "dataSource": "sample_data",
  "dimension": "sample_dim",
  "threshold": 5,
  "metric": "count",
  "granularity": "all",
  "filter": {
    "type": "and",
    "fields": [
      {
        "type": "selector",
        "dimension": "dim1",
        "value": "some_value"
      },
      {
        "type": "selector",
        "dimension": "dim2",
        "value": "some_other_val"
      }
    ]
  },
  "aggregations": [
    {
      "type": "longSum",
      "name": "count",
      "fieldName": "count"
    },
    {
      "type": "doubleSum",
      "name": "some_metric",
      "fieldName": "some_metric"
    }
  ],
  "postAggregations": [
    {
      "type": "arithmetic",
      "name": "sample_divide",
      "fn": "/",
      "fields": [
        {
          "type": "fieldAccess",
          "name": "some_metric",
          "fieldName": "some_metric"
        },
        {
          "type": "fieldAccess",
          "name": "count",
          "fieldName": "count"
        }
      ]
    }
  ],
  "intervals": [
    "2013-08-31T00:00:00.000/2013-09-03T00:00:00.000"
  ]
}
|]

groupByQueryV :: Value
groupByQueryV = [aesonQQ|
{
   "filter" : {
      "fields" : [
         {
            "dimension" : "carrier",
            "type" : "selector",
            "value" : "AT&T"
         },
         {
            "type" : "or",
            "fields" : [
               {
                  "dimension" : "make",
                  "value" : "Apple",
                  "type" : "selector"
               },
               {
                  "dimension" : "make",
                  "type" : "selector",
                  "value" : "Samsung"
               }
            ]
         }
      ],
      "type" : "and"
   },
   "queryType" : "groupBy",
   "dimensions" : [
      "country",
      "device"
   ],
   "aggregations" : [
      {
         "fieldName" : "user_count",
         "type" : "longSum",
         "name" : "total_usage"
      },
      {
         "name" : "data_transfer",
         "type" : "doubleSum",
         "fieldName" : "data_transfer"
      }
   ],
   "dataSource" : "sample_datasource",
   "limitSpec" : {
      "limit" : 5000,
      "columns" : [
         "country",
         "data_transfer"
      ],
      "type" : "default"
   },
   "intervals" : [
      "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000"
   ],
   "having" : {
      "value" : 100,
      "type" : "greaterThan",
      "aggregation" : "total_usage"
   },
   "postAggregations" : [
      {
         "name" : "avg_usage",
         "type" : "arithmetic",
         "fields" : [
            {
               "type" : "fieldAccess",
               "fieldName" : "data_transfer"
            },
            {
               "fieldName" : "total_usage",
               "type" : "fieldAccess"
            }
         ],
         "fn" : "/"
      }
   ],
   "granularity" : "day"
}
|]

suite :: Spec
suite = 
    describe "ToJSON for Query" $ do
        it "has correct output for known TimeSeries" $ do
            diff (toJSON timeSeriesQueryQ) timeSeriesQueryV `shouldBe` Patch []
        it "has correct output for known TopN"  pending
        it "has correct output for known GroupBy"  pending
        it "has correct output for many combinations"  pending
