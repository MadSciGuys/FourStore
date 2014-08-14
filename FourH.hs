{-|
Module      : FourH
Description : Library for Efficient Inter-Process Communication with 4store
Copyright   : Travis Whitaker 2014
License     : All rights reserved.
Maintainer  : twhitak@its.jnj.com
Stability   : Provisional
Portability : POSIX

FourH is a library for using 4store's efficient native binary protocol instead of the SPARQL
protocol. This has the additional benefit of allowing query results to be read lazily (just like
HDBC allows for SQL databases, or normal Haskell file IO). Note that this requires the ability to
execute the "4s-query" client on a node in the 4store cluster, e.g. via local sockets or SSH.
Although this package does not make use of the SPARQL protocol, 'HSparql.QueryGenerator' can be
used in conjunction with FourH. For example:

 > import FourH
 > import Database.HSparql.QueryGenerator
 >
 > selectAll = do
 >     s <- var
 >     p <- var
 >     o <- var
 >     triple s p o
 >     return SelectQuery { queryVars = [s, p, o] }
 >
 > main = do
 >     connection <- fsSelectConnect "knowledge_base_name" Othree
 >     result     <- fsSelectQuery connection $ createSelectQuery selectAll
 >     -- ~~~ --
 >     fsClose connection
-}

{-# LANGUAGE OverloadedStrings #-}

module FourH (

    -- * Types

    KBname
   ,OptLevel(..)
   ,FsHandle

    -- * Connection Handling

   ,fsSelectConnect
   ,fsConstructConnect
   ,fsKill
   ,fsSafeTerminate
   ,fsClose

    -- * Query Execution

   ,fsSelectQuery
   ,fsConstructQuery
   ,fsStrictSelectQuery

) where

import Control.DeepSeq (deepseq)

import qualified Data.ByteString.Lazy.Char8 as C (ByteString, split, lines, hGetContents)

import Data.List (init, tail)

import System.Exit (ExitCode(..))
import System.IO (Handle, hReady, hGetContents, hPutStr, hFlush, hClose, hSetBinaryMode, hGetLine)
import System.Process (ProcessHandle, CreateProcess(..), StdStream(CreatePipe), createProcess,
                       proc, terminateProcess, waitForProcess)

-- | Knowledge base name.
type KBname   = String

-- | Query optimization level (0-3).
data OptLevel = Ozero | Oone | Otwo | Othree

type FsInput  = Handle
type FsOutput = Handle
type FsError  = Handle
type FsProc   = ProcessHandle
data FsMode = Select | Construct

-- | Abstract reference to a 4s-query process. An 'FsHandle' is created by the 'fsSelectConnect' or
--   'fsConstructConnect' functions, depending on what sort of query will be executed.
newtype FsHandle = FsHandle { getHandles :: (FsInput, FsOutput, FsError, FsProc, FsMode) }

fsInput :: FsHandle -> FsInput
fsInput = (\(x, _, _, _, _) -> x) . getHandles

fsOutput :: FsHandle -> FsOutput
fsOutput = (\(_, x, _, _, _) -> x) . getHandles

fsError :: FsHandle -> FsError
fsError = (\(_, _, x, _, _) -> x) . getHandles

fsProc :: FsHandle -> FsProc
fsProc = (\(_, _, _, x, _) -> x) . getHandles

fsMode :: FsHandle -> FsMode
fsMode = (\(_, _, _, _, x) -> x) . getHandles

optString :: OptLevel -> String
optString Ozero  = "-O0"
optString Oone   = "-O1"
optString Otwo   = "-O2"
optString Othree = "-O3"

-- | Spawn a 4s-query process in select mode connected to the specified knowledge base and running
--   with the provided optimization level. In order to safely implement lazy result fetching, each
--   'FsHandle' is only usable for one query; subsequent select queries (even to the same KB) must
--   be executed with a new 'FsHandle'.
fsSelectConnect :: KBname -> OptLevel -> IO FsHandle
fsSelectConnect kbname optlevel = do
    (Just fsin, Just fsout, Just fserr, ph) <-
        createProcess (proc "4s-query" ["-P", "-s", "-1", "-f", "text", optString optlevel, kbname])
            { std_in = CreatePipe, std_out = CreatePipe, std_err = CreatePipe }
    anyStdOut <- hReady fsout
    anyStdErr <- hReady fserr
    if anyStdOut then hGetContents fsout >>= fail else
        if anyStdErr then hGetContents fserr >>= fail else
            hSetBinaryMode fsin True  >>
            hSetBinaryMode fsout True >>
            return (FsHandle (fsin, fsout, fserr, ph, Select))

-- | Spawn a 4s-query process in construct mode connected to the specified knowledge base and
--   running with the provided optimization level. 4s-query processes created by this function will
--   insert the immediate results of any construct query into the knowledge base and will not be
--   accessible from Haskell. Unlike select mode 'FsHandle's, construct queries will block until
--   the KB being inserted into is consistent and are therefore /reusable/.
fsConstructConnect :: KBname -> OptLevel -> IO FsHandle
fsConstructConnect kbname optlevel = do
    (Just fsin, Just fsout, Just fserr, ph) <-
        createProcess (proc "4s-query" ["-P", "-s", "-1", "-I", optString optlevel, kbname])
            { std_in = CreatePipe, std_out = CreatePipe, std_err = CreatePipe }
    anyStdOut <- hReady fsout
    anyStdErr <- hReady fserr
    if anyStdOut then hGetContents fsout >>= fail else
        if anyStdErr then hGetContents fserr >>= fail else
            hSetBinaryMode fsin True  >>
            hSetBinaryMode fsout True >>
            return (FsHandle (fsin, fsout, fserr, ph, Construct))

-- | Kill the 4s-query process referenced by the provided 'FsHandle'.
fsKill :: FsHandle -> IO ExitCode
fsKill fsh = terminateProcess fsp >> waitForProcess fsp
    where fsp = fsProc fsh

-- | Send EOF to the 4s-query process referenced by the provided 'FsHandle' and wait for it to
--   terminate. Depending on the state of 4s-query's parser, 'ExitFailure' may still be returned.
--   This function is only usable on select mode 'FsHandle's that haven't satisfied a query yet or
--   construct mode 'FsHandle's that aren't blocking.
fsSafeTerminate :: FsHandle -> IO ExitCode
fsSafeTerminate fsh = hPutStr fsi "\EOT" >>
                      hFlush fsi         >>
                      hClose fsi         >>
                      waitForProcess fsp
                        where fsp = fsProc fsh
                              fsi = fsInput fsh

-- | Safely terminate a select mode 4s-query process that has already answered a query. This
--   function will block if there is remaining demand for the 4s-query process' results elsewhere in
--   the program. 'fsClose' is to 'fsSelectQuery' as 'hClose' is to 'hGetContents'.
fsClose :: FsHandle -> IO ExitCode
fsClose = waitForProcess . fsProc

-- | Execute the provided query on the provided 'FsHandle'. The remainder of the 4s-query process'
--   life is dedicated to handling on-demand lazy evaluation, so each select mode 'FsHandle' is
--   effectively only good for one query; attempting to re-use an 'FsHandle' will raise an IO
--   Exception. Once the entire query result has been thunk'd, the 4s-query process remains until
--   'fsClose' is called.
--
--   Each record is returned as a list of 'C.Bytestring's representing the variables bound in the
--   query's select statement.
fsSelectQuery :: FsHandle -> String -> IO [[C.ByteString]]
fsSelectQuery (FsHandle (_, _, _, _, Construct)) _ = fail "Wrong FsHandle Mode (Construct->Select)"
fsSelectQuery fsh q = do
    hPutStr (fsInput fsh) (q ++ "\n#EOQ\n")
    hFlush (fsInput fsh)
    hClose (fsInput fsh)
    output <- C.hGetContents (fsOutput fsh)
    return ((map (C.split '\t') . tail . takeWhile (/= "#EOR") . C.lines) output)

-- | Execute the provided query on the provided 'FsHandle'. This function will block until the KB
--   being queried is consistent, i.e. all of the triples generated by the query are inserted.
--   Unlike select mode 'FsHandle's, construct mode 'FsHandle's are reusable; query results aren't
--   immediately available in Haskell so lazy evaluation need not be considered.
fsConstructQuery :: FsHandle -> String -> IO ()
fsConstructQuery (FsHandle (_, _, _, _, Select)) _ = fail "Wrong FsHandle Mode (Select->Construct)"
fsConstructQuery fsh q = do
    hPutStr (fsInput fsh) (q ++ "\n#EOQ\n")
    hFlush (fsInput fsh)
    hGetLine (fsOutput fsh)
    return ()

-- | Execute the provided query against the provided KB at the provided optimization level. This
--   function creates a one-time use 'FsHandle', strictly evaluates the query results, and closes
--   the 'FsHandle'. This is often useful when a series of short queries must be run sequentially
--   and the full result set is required soon after.
fsStrictSelectQuery :: KBname -> OptLevel -> String -> IO [[C.ByteString]]
fsStrictSelectQuery kb opt q = do
    fsh <- fsSelectConnect kb opt
    results <- fsSelectQuery fsh q
    results `deepseq` fsClose fsh
    return results
