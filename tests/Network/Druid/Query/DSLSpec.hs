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

module Network.Druid.Query.DSLSpec where

import Test.Hspec
import Control.Applicative
import Data.Aeson
import Network.Druid.Query.DSL
import Network.Druid.Query.AST
import Network.Druid.Query.ASTSpec(groupByQueryQ)
import Pipes
import qualified Pipes.Prelude as P

data Phones = Phones

data Carrier = Carrier
data Make = Make
data Usage = Usage

data UsageSum = UsageSum
data Count = Count

instance DataSource Phones where
    dataSourceName _ = "phone_source"
instance Dimension Carrier where
    dimensionName _ = "carrier"
instance Dimension Make where
    dimensionName _ = "make"
instance Metric Usage where
    metricName _ = "usage"
instance MetricVar UsageSum where
    metricVarName _ = "total_usage"
instance MetricVar Count where
    metricVarName _ = "count"

instance HasDimension Phones Carrier
instance HasDimension Phones Make
instance HasMetric Phones Usage

spec :: Spec
spec = 
    describe "ideal DSL" $ do
        it "builds correct query" $ do
            groupByQuery Phones [SomeDimension Carrier] GranularityDay [] $ do
                applyFilter $ filterAnd
                    [ filterAnd [filterSelector Carrier "AT&T"]
                    , filterSelector Carrier "AT&T"
                    , filterSelector Make "Samsung"
                    ]
                longSum Usage UsageSum
            `shouldBe` groupByQueryQ

        it "decodes things it should" $ do
            let query = (,) <$> longSum Usage UsageSum <*> count Count
            r <- groupBy "http://localhost:8084/druid/v2/"
                         Phones
                         [SomeDimension Carrier]
                         GranularityDay
                         []
                         query $ \(_, cnt) look pipe -> do
                runEffect $
                    pipe
                    >-> (await >>= \v -> yield $ look cnt v)
                    >-> P.print
                
                
                
            r `shouldBe` ()
