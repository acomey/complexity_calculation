
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
import           Control.Distributed.Process.Node                   (initRemoteTable)
import           Control.Monad
import           Network.Transport.TCP                              (createTransport,
                                                                     defaultTCPParameters)
import           PrimeFactors
import           System.Environment                                 (getArgs)
import           System.Exit
import           Argon
import           System.Process



getComplexity :: ComplexityBlock -> Int
getComplexity ( CC tuple ) = getComplexity' tuple

getComplexity' :: (Loc, String, Int) -> Int
getComplexity' ( _,_,complexity ) = complexity




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

filterHaskellFile :: [FilePath] ->[FilePath]
filterHaskellFile fileList = filter (".hs" `isSuffixOf`) fileList


filterFiles :: FilePath ->IO [FilePath]
filterFiles path = do
    root <- listDirectory path
    --putStrLn $ show root
    --
    printList path root
    return root

    where

      printList :: FilePath->[FilePath] -> IO ()
      printList base root = do
        forM_ root $ \name -> do
          let b = (base </> name)
          isFile<- liftIO $ doesFileExist $ base </> name
          if not isFile then do
              result <- listDirectory $ base </> name

              printList (base </> name) result
              --putStrLn $ show a
          else do
              putStrLn $ show "hello"
              --putStrLn $ base </> name


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

      -- Wait for work to arrive. We will either be sent a message with an integer value to use as input for processing,
      -- or else we will be sent (). If there is work, do it, otherwise terminate
      receiveWait
        [ match $ \path  -> do
            liftIO $ putStrLn $ "[Node " ++ (show us) ++ "] given work: "
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

manager :: String    -- The number range we wish to generate work for (there will be n work packages)
        -> [NodeId]   -- The set of cloud haskell nodes we will initalise as workers
        -> Process Integer

manager url workers = do
  us <- getSelfPid

  -- first, we create a thread that generates the work tasks in response to workers
  -- requesting work.
  workQueue <- spawnLocal $ do
    -- Return the next bit of work to be done

    callCommand $ "git clone " ++ url
    let gitName = last $ splitOn "/" url
    let name = head $ splitOn "." gitName

    files <-traverseDir ("./" ++ name)



    let numFiles = length (filterHaskellFile files)
    forM_ (filterHaskellFile files) $ \path -> do
      them <- expect   -- await a message from a free worker asking for work
      send them path     -- send them work

    forM_ [1 .. n] $ \m -> do
      pid <- expect   -- await a message from a free worker asking for work
      send pid m     -- send them work
>>>>>>> a9b04c75b92e06638dc71f79a509859b159d2187

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
  complexityResult <- do
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
