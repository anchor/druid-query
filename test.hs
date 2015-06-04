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
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE QuasiQuotes #-}

module Network.Druid.Query.DSLSpec where

import Control.Applicative
import Data.Aeson
import Data.Time.QQ
import Data.Scientific
import Network.Druid.Query.DSL
import Network.Druid.Query.AST

data Nagios = Nagios

data Count = Count

data NagiosMetric = NagiosMetric
instance Dimension NagiosMetric where dimensionName _ = "metric"
instance HasDimension Nagios NagiosMetric

instance DataSource Nagios where
    dataSourceName _ = "rabbitmq_nagios"

instance MetricVar Count where
    metricVarName _ = "count"


main :: IO ()
main =  do
    let q = groupByQuery
                 Nagios
                 [SomeDimension NagiosMetric]
                 GranularityDay
                 [Interval [utcIso8601| 2015-05-08 |] [utcIso8601| 2015-05-10 |]]
                 (count Count)

    runQuery "http://localhost:8084/druid/v2/" q >>= print
