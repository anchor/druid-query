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
import Pipes
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
    groupBy "http://localhost:8084/druid/v2/"
                 Nagios
                 [SomeDimension NagiosMetric]
                 GranularityDay
                 [Interval [utcIso8601| 2015-04-08 |] [utcIso8601| 2015-06-09 |]]
                 (count Count) $ \cnt look pipe ->
        runEffect $ pipe >-> (await >>= \v -> liftIO $ print $ look cnt v)
