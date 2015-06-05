--
-- Copyright © 2013-2015 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE OverloadedStrings #-}

-- | A library and DSL for making Druid queries.
--
-- There are two ways of making a query. Either build up your AST manually with
-- 'Druid.Query.AST' to produce a 'Query' value, or, use the DSL exported by
-- default (with 'groupByQuery' and friends).
module Network.Druid.Query
(
    -- * Running queries
    runQuery,
    module Network.Druid.Query.DSL,
) where

import Control.Applicative
import Data.Aeson
import Data.ByteString.Lazy as LBS
import Network.Druid.Query.AST
import Network.Druid.Query.DSL
import Network.HTTP.Client
import Network.HTTP.Client.TLS

-- | Execute a druid query, parsing the resulting JSON response is up to you.
runQuery :: String -> Query -> IO (Either String Value)
runQuery uri query = do
    req <- parseUrl uri
    let req' = req { method = "POST"
                   , requestBody = RequestBodyLBS (encode query)
                   }

    withManager tlsManagerSettings $ \m ->
        withResponse req' m $ \resp ->
            -- Do not stream, Druid will not stream, plus we're dealing with
            -- JSON so let's just not go there.
            eitherDecode . LBS.fromChunks <$> brConsume (responseBody resp)


