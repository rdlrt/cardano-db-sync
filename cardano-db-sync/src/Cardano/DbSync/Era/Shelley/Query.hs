{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Cardano.DbSync.Era.Shelley.Query
  ( queryPoolHashId
  , queryStakeAddress
  , queryStakePoolKeyHash
  , queryStakeAddressRef
  , queryResolveInput

  , queryStakeAddressIdPair
  , queryPoolHashIdPair
  , queryPoolUpdateByBlock
  ) where

import           Cardano.Prelude hiding (from, maybeToEither, on)

import           Cardano.Db
import qualified Cardano.DbSync.Era.Shelley.Generic as Generic

import qualified Cardano.Ledger.Keys as Ledger

import qualified Cardano.Sync.Era.Shelley.Generic as Generic
import           Cardano.Sync.Util

import           Cardano.Slotting.Slot (SlotNo (..))

import           Database.Esqueleto (InnerJoin (..), Value (..), desc, from, just, limit, on,
                   orderBy, select, val, where_, (==.), (^.))
import           Database.Persist.Sql (SqlBackend)

import           Ouroboros.Consensus.Cardano.Block (StandardCrypto)

import qualified Shelley.Spec.Ledger.Address as Shelley
import           Shelley.Spec.Ledger.Credential (Ptr (..), StakeReference (..))


queryPoolHashId :: MonadIO m => ByteString -> ReaderT SqlBackend m (Maybe PoolHashId)
queryPoolHashId hash = do
  res <- select . from $ \ phash -> do
            where_ (phash ^. PoolHashHashRaw ==. val hash)
            pure (phash ^. PoolHashId)
  pure $ unValue <$> listToMaybe res


queryStakeAddress
    :: MonadIO m
    => ByteString
    -> ReaderT SqlBackend m (Either LookupFail StakeAddressId)
queryStakeAddress addr = do
  res <- select . from $ \ saddr -> do
            where_ (saddr ^. StakeAddressHashRaw ==. val addr)
            pure (saddr ^. StakeAddressId)
  pure $ maybeToEither (DbLookupMessage $ "StakeAddress " <> renderByteArray addr) unValue (listToMaybe res)

queryStakePoolKeyHash
    :: forall era m. MonadIO m
    => Ledger.KeyHash 'Ledger.StakePool era
    -> ReaderT SqlBackend m (Either LookupFail PoolHashId)
queryStakePoolKeyHash kh = do
  res <- select . from $ \ (poolUpdate `InnerJoin` poolHash `InnerJoin` tx `InnerJoin` blk) -> do
            on (blk ^. BlockId ==. tx ^. TxBlockId)
            on (tx ^. TxId ==. poolUpdate ^. PoolUpdateRegisteredTxId)
            on (poolUpdate ^. PoolUpdateHashId ==. poolHash ^. PoolHashId)
            where_ (poolHash ^. PoolHashHashRaw ==. val (Generic.unKeyHashRaw kh))
            orderBy [desc (blk ^. BlockSlotNo)]
            limit 1
            pure (poolHash ^. PoolHashId)
  pure $ maybeToEither (DbLookupMessage "StakePoolKeyHash") unValue (listToMaybe res)

queryStakeAddressRef
    :: MonadIO m
    => Shelley.Addr StandardCrypto
    -> ReaderT SqlBackend m (Maybe StakeAddressId)
queryStakeAddressRef addr =
    case addr of
      Shelley.AddrBootstrap {} -> pure Nothing
      Shelley.Addr nw _pcred sref ->
        case sref of
          StakeRefBase cred -> do
            eres <- queryStakeAddress $ Shelley.serialiseRewardAcnt (Shelley.RewardAcnt nw cred)
            pure $ either (const Nothing) Just eres
          StakeRefPtr (Ptr slotNo txIx certIx) -> queryStakeDelegation slotNo txIx certIx
          StakeRefNull -> pure Nothing
  where
    queryStakeDelegation
        :: MonadIO m
        => SlotNo -> Natural -> Natural
        -> ReaderT SqlBackend m (Maybe StakeAddressId)
    queryStakeDelegation (SlotNo slot) txIx certIx = do
      res <- select . from $ \ (blk `InnerJoin` tx `InnerJoin` dlg) -> do
                on (tx ^. TxId ==. dlg ^. DelegationTxId)
                on (blk ^. BlockId ==. tx ^. TxBlockId)
                where_ (blk ^. BlockSlotNo ==. just (val slot))
                where_ (tx ^. TxBlockIndex ==. val (fromIntegral txIx))
                where_ (dlg ^. DelegationCertIndex ==. val (fromIntegral certIx))
                -- Need to order by BlockSlotNo descending for correct behavior when there are two
                -- or more delegation certificates in a single epoch.
                orderBy [desc (blk ^. BlockSlotNo)]
                limit 1
                pure (dlg ^. DelegationAddrId)
      pure $ unValue <$> listToMaybe res

queryResolveInput :: MonadIO m => Generic.TxIn -> ReaderT SqlBackend m (Either LookupFail (TxId, DbLovelace))
queryResolveInput txIn = do
    queryTxOutValue (Generic.txInHash txIn, fromIntegral (Generic.txInIndex txIn))

queryStakeAddressIdPair :: MonadIO m => Generic.StakeCred -> ReaderT SqlBackend m (Maybe (Generic.StakeCred, StakeAddressId))
queryStakeAddressIdPair cred@(Generic.StakeCred bs) = do
    res <- select . from $ \ saddr -> do
              where_ (saddr ^. StakeAddressHashRaw ==. val bs)
              pure $ saddr ^. StakeAddressId
    pure $ convert <$> listToMaybe res
  where
    convert :: Value StakeAddressId -> (Generic.StakeCred, StakeAddressId)
    convert (Value said) = (cred, said)

queryPoolHashIdPair
    :: MonadIO m
    => Ledger.KeyHash 'Ledger.StakePool StandardCrypto
    -> ReaderT SqlBackend m (Maybe (Ledger.KeyHash 'Ledger.StakePool StandardCrypto, PoolHashId))
queryPoolHashIdPair pkh = do
    res <- select . from $ \ pool -> do
              where_ (pool ^. PoolHashHashRaw ==. val (Generic.unKeyHashRaw pkh))
              pure $ pool ^. PoolHashId
    pure $ convert <$> listToMaybe res
  where
    convert :: Value PoolHashId -> (Ledger.KeyHash 'Ledger.StakePool StandardCrypto, PoolHashId)
    convert (Value phid) = (pkh, phid)

-- Check if there are other PoolUpdates in the same blocks for the same pool
queryPoolUpdateByBlock :: MonadIO m => BlockId -> PoolHashId -> ReaderT SqlBackend m Bool
queryPoolUpdateByBlock blkId poolHashId = do
    res <- select . from $ \ (poolUpdate `InnerJoin` tx `InnerJoin` blk) -> do
              on (blk ^. BlockId ==. tx ^. TxBlockId)
              on (tx ^. TxId ==. poolUpdate ^. PoolUpdateRegisteredTxId)
              where_ (poolUpdate ^. PoolUpdateHashId ==. val poolHashId)
              where_ (blk ^. BlockId ==. val blkId)
              limit 1
              pure (blk ^. BlockEpochNo)
    pure $ not (null res)
