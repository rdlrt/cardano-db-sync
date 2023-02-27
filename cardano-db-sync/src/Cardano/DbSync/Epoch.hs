{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Cardano.DbSync.Epoch (
  epochStartup,
  epochInsert,
) where

import Cardano.BM.Trace (logError, logInfo)
import qualified Cardano.Chain.Block as Byron
import Cardano.Db (EntityField (..), EpochId)
import qualified Cardano.Db as DB
import Cardano.DbSync.Api
import Cardano.DbSync.Error
import Cardano.DbSync.Types
import Cardano.DbSync.Util
import Cardano.Prelude hiding (from, on, replace)
import Cardano.Slotting.Slot (EpochNo (..))
import Control.Concurrent.Class.MonadSTM.Strict (
  readTVarIO,
  writeTVar,
 )
import Control.Monad.Extra (whenJust)
import Control.Monad.Logger (LoggingT)
import Control.Monad.Trans.Control (MonadBaseControl)
import Database.Esqueleto.Experimental (
  SqlBackend,
  desc,
  from,
  limit,
  orderBy,
  replace,
  select,
  table,
  unValue,
  val,
  where_,
  (==.),
  (^.),
 )
import Ouroboros.Consensus.Byron.Ledger (ByronBlock (..))
import Ouroboros.Consensus.Cardano.Block (HardForkBlock (..))

-- Populating the Epoch table has two mode:
--  * SyncLagging: when the node is far behind the chain tip and is just updating the DB. In this
--    mode, the row for an epoch is only calculated and inserted when at the end of the epoch.
--  * Following: When the node is at or close to the chain tip, the row for a given epoch is
--    updated on each new block.
--
-- When in syncing mode, the row for the current epoch being synced may be incorrect.
epochStartup :: SyncEnv -> SqlBackend -> IO ()
epochStartup env backend =
  when isExtended $ do
    DB.runDbIohkLogging backend trce $ do
      liftIO . logInfo trce $ "epochStartup: Checking"
      mlbe <- queryLatestEpochNo
      whenJust mlbe $ \lbe -> do
        let backOne = if lbe == 0 then 0 else lbe - 1
        liftIO $ atomically $ writeTVar var (Just backOne)
  where
    isExtended = soptExtended $ envOptions env
    trce = getTrace env
    var = envEpochTable env

epochInsert :: SyncEnv -> BlockDetails -> ReaderT SqlBackend (LoggingT IO) (Either SyncNodeError ())
epochInsert env (BlockDetails cblk details) = do
  case cblk of
    BlockByron bblk ->
      case byronBlockRaw bblk of
        Byron.ABOBBoundary {} ->
          -- For the OBFT era there are no boundary blocks so we ignore them even in
          -- the Ouroboros Classic era.
          pure $ Right ()
        Byron.ABOBBlock _blk ->
          insertBlock env details
    BlockShelley {} -> epochUpdate
    BlockAllegra {} -> epochUpdate
    BlockMary {} -> epochUpdate
    BlockAlonzo {} -> epochUpdate
    BlockBabbage {} -> epochUpdate
  where
    trce = getTrace env
    -- What we do here is completely independent of Shelley/Allegra/Mary eras.
    epochUpdate :: ReaderT SqlBackend (LoggingT IO) (Either SyncNodeError ())
    epochUpdate = do
      when (sdSlotTime details > sdCurrentTime details) $
        liftIO . logError trce $
          mconcat
            ["Slot time '", textShow (sdSlotTime details), "' is in the future"]
      insertBlock env details

-- -------------------------------------------------------------------------------------------------

insertBlock ::
  SyncEnv ->
  SlotDetails ->
  ReaderT SqlBackend (LoggingT IO) (Either SyncNodeError ())
insertBlock env details = do
  mLatestCachedEpoch <- liftIO $ readTVarIO $ envEpochTable env
  let lastCachedEpoch = fromMaybe 0 mLatestCachedEpoch
      epochNum = unEpochNo (sdEpochNo details)

  -- These cases are listed from the least likely to occur to the most
  -- likley to keep the logic sane.

  if
      | epochNum > 0 && isNothing mLatestCachedEpoch ->
          updateEpochNum 0 env
      | epochNum >= lastCachedEpoch + 2 ->
          updateEpochNum (lastCachedEpoch + 1) env
      | getSyncStatus details == SyncFollowing ->
          -- Following the chain very closely.
          updateEpochNum epochNum env
      | otherwise ->
          pure $ Right ()

updateEpochNum :: (MonadBaseControl IO m, MonadIO m) => Word64 -> SyncEnv -> ReaderT SqlBackend m (Either SyncNodeError ())
updateEpochNum epochNum env = do
  mid <- queryEpochId epochNum
  res <- maybe insertEpoch updateEpoch mid
  liftIO $ atomically $ writeTVar (envEpochTable env) (Just epochNum)
  pure res
  where
    updateEpoch :: MonadIO m => EpochId -> ReaderT SqlBackend m (Either SyncNodeError ())
    updateEpoch epochId = do
      epoch <- DB.queryCalcEpochEntry epochNum
      Right <$> replace epochId epoch

    insertEpoch :: (MonadBaseControl IO m, MonadIO m) => ReaderT SqlBackend m (Either SyncNodeError ())
    insertEpoch = do
      epoch <- DB.queryCalcEpochEntry epochNum
      liftIO . logInfo trce $ "epochPluginInsertBlockDetails: epoch " <> textShow epochNum
      void $ DB.insertEpoch epoch
      pure $ Right ()

    trce = getTrace env

-- | Get the PostgreSQL row index (EpochId) that matches the given epoch number.
queryEpochId :: MonadIO m => Word64 -> ReaderT SqlBackend m (Maybe EpochId)
queryEpochId epochNum = do
  res <- select $ do
    epoch <- from $ table @DB.Epoch
    where_ (epoch ^. DB.EpochNo ==. val epochNum)
    pure (epoch ^. EpochId)
  pure $ unValue <$> listToMaybe res

-- | Get the epoch number of the most recent epoch in the Epoch table.
queryLatestEpochNo :: MonadIO m => ReaderT SqlBackend m (Maybe Word64)
queryLatestEpochNo = do
  res <- select $ do
    epoch <- from $ table @DB.Epoch
    orderBy [desc (epoch ^. DB.EpochNo)]
    limit 1
    pure (epoch ^. DB.EpochNo)
  pure $ unValue <$> listToMaybe res
