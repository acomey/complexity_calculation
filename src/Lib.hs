
{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE TemplateHaskell #-}
--{-# CPP #-}

-- | use-haskell
-- The purpose of this project is to provide a baseline demonstration of the use of cloudhaskell in the context of the
-- code complexity measurement individual programming project. The cloud haskell platform provides an elegant set of
-- features that support the construction of a wide variety of multi-node distributed systems commuinication
-- architectures. A simple message passing abstraction forms the basis of all communication.
--
-- This project provides a command line switch for starting the application in master or worker mode. It is implemented
-- using the work-pushing pattern described in http://www.well-typed.com/blog/71/. Comments below describe how it
-- operates. A docker-compose.yml file is provided that supports the launching of a master and set of workers.

module Lib
    ( someFunc
    ) where

-- These imports are required for Cloud Haskell
import           Control.Distributed.Process
import           Control.Distributed.Process.Backend.SimpleLocalnet
import           Control.Distributed.Process.Closure
import           Control.Monad
import           Control.Monad.IO.Class       (liftIO)
import           Control.Monad.Trans.Except   (ExceptT)
import           Control.Monad.Trans.Resource
import           Data.Aeson
import           Data.Aeson.TH
import qualified Data.ByteString.Lazy         as L
import qualified Data.List                    as DL
import           Data.Maybe                   (catMaybes)
import           Data.Text                    (pack, unpack)
import           Data.Time.Clock              (UTCTime, getCurrentTime)
import           Data.Time.Format             (defaultTimeLocale, formatTime)
import           Database.MongoDB
import           GHC.Generics
import           Network.HTTP.Client          (defaultManagerSettings,
                                               newManager)
import           Network.Wai
import           Network.Wai.Handler.Warp
import           Network.Wai.Logger
import           Servant
import qualified Servant.API                  as SC
import qualified Servant.Client               as SC
import           System.Environment           (getArgs, getProgName, lookupEnv)
import           System.Log.Formatter
import           System.Log.Handler           (setFormatter)
import           System.Log.Handler.Simple
import           System.Log.Handler.Syslog
import           System.Log.Logger
import           System.Process

import           Argon
import           System.Console.Docopt
import           Pipes
import           Pipes.Safe (runSafeT)
import qualified Pipes.Prelude as P
import           Control.Monad (forM_,forM)
import           System.Directory (getDirectoryContents)
import           System.Directory (doesFileExist)
import           System.Directory (listDirectory)
import           System.FilePath.Posix
import           Data.List (isSuffixOf)
import           Data.List.Split
import           Network.HTTP(simpleHTTP,getRequest,getResponseBody,getResponseCode,ResponseCode)
import           Control.Distributed.Process.Node                   (initRemoteTable)



-- returns all the files in a Directory
traverseDir :: FilePath -> IO [FilePath]
traverseDir top = do
    ds <- listDirectory top
    paths <- forM ds $ \d -> do
      let path = top </> d
      isFile<-liftIO $ doesFileExist path
      if not isFile
        then traverseDir path
        else return [path]
    return (concat paths)

-- returns the third element. Argon analyze outputs three results; the third of which is the complexity result
getComplexity' :: (Loc, String, Int) -> Int
getComplexity' ( _,_,complexity ) = complexity


-- returns haskell files from a list of files
filterHaskellFile :: [FilePath] ->[FilePath]
filterHaskellFile fileList = filter (".hs" `isSuffixOf`) fileList


-- Config is one of the paramters of Argon's analyze function
getConfig ::Config
getConfig = Config {
    minCC       = read "2"
  , exts        = []
  , headers     = []
  , includeDirs = []
  , outputMode  = JSON
  }




-- this is the work we get workers to do. It could be anything we want. To keep things simple, we'll calculate the
-- number of prime factors for the integer passed.

-- | worker function.
-- This is the function that is called to launch a worker. It loops forever, asking for work, reading its message queue
-- and sending the result of runnning numPrimeFactors on the message content (an integer).
worker :: ( ProcessId  -- The processid of the manager (where we send the results of our work)
         , ProcessId) -- the process id of the work queue (where we get our work from)
       -> Process ()
worker (manager, workQueue) = do
    us <- getSelfPid              -- get our process identifier
    liftIO $ putStrLn $ "Starting worker: " ++ show us
    go us
  where
    go :: ProcessId -> Process ()
    go us = do

      send workQueue us -- Ask the queue for work. Note that we send out process id so that a message can be sent to us

      -- Wait for work to arrive.
     --  We will either be sent a string message  as input for processing,
      -- or else we will be sent (). If there is work, do it, otherwise terminate
      receiveWait
        [ match $ \path  -> do
            liftIO $ putStrLn $ "[Node " ++ (show us) ++ "] given work: "
            -- analyzes the file at that path
            (file,analysis) <- analyze getConfig path
            let result = getComplexity' analysis
            send manager result
            liftIO $ putStrLn $ "[Node " ++ (show us) ++ "] finished work."
            go us -- note the recursion this function is called again!
        , match $ \ () -> do
            liftIO $ putStrLn $ "Terminating node: " ++ show us
            return ()
        ]

remotable ['worker] -- this makes the worker function executable on a remote node

manager :: String    -- The url of the repository we wish to analyze
        -> [NodeId]   -- The set of cloud haskell nodes we will initalise as workers
        -> Process Integer

manager url workers = do
  us <- getSelfPid

  -- first, we create a thread that generates the work tasks in response to workers
  -- requesting work.
  workQueue <- spawnLocal $ do
    -- clones the repository
    callCommand $ "git clone " ++ url
    -- separates the repo name from the url
    let gitName = last $ splitOn "/" url
    let name = head $ splitOn "." gitName
    files <-traverseDir ("./" ++ name)

    let numFiles = length (filterHaskellFile files)
    -- 
    forM_ (filterHaskellFile files) $ \path -> do
      them <- expect   -- await a message from a free worker asking for work
      send them path     -- send them work

    forM_ [1 .. numFiles] $ \m -> do
      pid <- expect   -- await a message from a free worker asking for work
      send pid m     -- send them work

    -- Once all the work is done tell the workers to terminate. We do this by sending every worker who sends a message
    -- to us a null content: () . We do this only after we have distributed all the work in the forM_ loop above. Note
    -- the indentiation - this is part of the workQueue do block.
    forever $ do
      pid <- expect
      send pid ()

  -- Next, start worker processes on the given cloud haskell nodes. These will start
  -- asking for work from the workQueue thread immediately.
  forM_ workers $ \ nid -> spawn nid ($(mkClosure 'worker) (us, workQueue))
  liftIO $ putStrLn $ "[Manager] Workers spawned"
  -- wait for all the results from the workers and return the sum total. Look at the implementation, whcih is not simply
  -- summing integer values, but instead is expecting results from workers.
  -- letcomplexityResult = liftIO $ do
  let gitName = last $ splitOn "/" url
  let name = head $ splitOn "." gitName
  files <-traverseDir ("./" ++ name)
  let numFiles = length (filterHaskellFile files)
  complexityList <- forM [1..numFiles] $ \m -> do
     newComplexity <- expect
     return newComplexity

  let result = ( sum complexityList )/numFiles
  return result
-- note how this function works: initialised with n, the number range we started the program with, it calls itself
-- recursively, decrementing the integer passed until it finally returns the accumulated value in go:acc. Thus, it will
-- be called n times, consuming n messages from the message queue, corresponding to the n messages sent by workers to
-- the manager message queue.


-- | This is the entrypoint for the program. We deal with program arguments and launch up the cloud haskell code from
-- here.

rtable :: RemoteTable
rtable = Lib.__remoteTable initRemoteTable

someFunc :: IO ()
someFunc = do


  args <- getArgs

  case args of
    ["manager", host, port, url] -> do
      putStrLn "Starting Node as Manager"
      backend <- initializeBackend host port rtable
      startMaster backend $ \workers -> do
        result <- manager (read url) workers
        liftIO $ print result
    ["worker", host, port] -> do
      putStrLn "Starting Node as Worker"
      backend <- initializeBackend host port rtable
      startSlave backend









  -- create a cloudhaskell node, which must be initialised with a network transport
  -- Right transport <- createTransport "127.0.0.1" "10501" defaultTCPParameters
  -- node <- newLocalNode transport initRemoteTable

  -- runProcess node $ do
  --   us <- getSelfNode
  --   _ <- spawnLocal $ sampleTask (1 :: Int, "using spawnLocal")
  --   pid <- spawn us $ $(mkClosure 'sampleTask) (1 :: Int, "using spawn")
  --   liftIO $ threadDelay 2000000
