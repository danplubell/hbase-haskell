{-# LANGUAGE OverloadedStrings #-}
module Database.Hbase.Client
(
    HBaseConnectionSource(..),
    HBaseColumnDescriptor(..),
    HBaseConnection(..),
    defaultHBaseConnectionSource,
    defaultColumnDescriptor,
    openConnection,
    closeConnection,
    createTable    
) where

import qualified    Data.ByteString.Lazy        as BL
import qualified    Data.ByteString.Char8       as BC
import qualified    Data.ByteString.Internal    as BSI
import qualified    Data.Text.Lazy              as TL
import              Network
import              GHC.IO.Handle.Types
import              Thrift.Transport.Handle
import              Thrift.Protocol.Binary
import              GHC.Word(Word8)
import              GHC.Int(Int32)
import              Database.Hbase.Internal.Hbase_Types
import qualified    Database.Hbase.Internal.Hbase_Client as HClient
import qualified    Data.Vector                 as Vector

------------Data Structures-------------------

data HBaseConnectionSource = HBaseConnectionSource
    {
          hostName  :: String
        , port      :: PortID 
    }deriving (Show)
    
defaultHBaseConnectionSource::HBaseConnectionSource    
defaultHBaseConnectionSource = HBaseConnectionSource
    {
          hostName  = "localhost"
        , port      = PortNumber 9090
    }


data HBaseColumnDescriptor  = HBaseColumnDescriptor 
    {
          columnName                :: Maybe String
        , columnMaxVersions         :: Maybe Int32
        , columnCompression         :: Maybe String
        , columnInMemory            :: Maybe Bool
        , columnBloomFilterType     :: Maybe String
        , columnFilterVectorSize    :: Maybe Int32
        , columnFilterNbHashes      :: Maybe Int32
        , columnCacheEnabled        :: Maybe Bool
        , columnTimeToLive          :: Maybe Int32
   }deriving (Show)

data HBaseConnection = HBaseConnection
    {
          handle              :: Handle
        , connectionSource    :: HBaseConnectionSource  
        , connectionProtocol  :: BinaryProtocol Handle
    }
defaultColumnDescriptor :: HBaseColumnDescriptor     
defaultColumnDescriptor=HBaseColumnDescriptor 
    {
          columnName                = Nothing
        , columnMaxVersions         = Nothing
        , columnCompression         = Nothing
        , columnInMemory            = Nothing
        , columnBloomFilterType     = Nothing
        , columnFilterVectorSize    = Nothing
        , columnFilterNbHashes      = Nothing
        , columnCacheEnabled        = Nothing
        , columnTimeToLive          = Nothing
    }       

type TableName = String  
type TableRegionName = String  
------------Functions-------------------------

openConnection :: HBaseConnectionSource -> IO HBaseConnection
openConnection c = do
    h <- hOpen (hostName c, port c)
    return $ HBaseConnection{ connectionSource = c, handle = h, connectionProtocol = BinaryProtocol h}

closeConnection :: HBaseConnection -> IO()
closeConnection c = tClose $ handle c
    
createTable :: TableName->[HBaseColumnDescriptor]->HBaseConnection-> IO()
createTable t l c =   
    HClient.createTable (connectionProtocol c,connectionProtocol c) 
                        (strToLazyByteString t) 
                        (Vector.fromList $ map fColDescFromHColDesc l)
    

enableTable :: TableName -> HBaseConnection ->IO()
enableTable = undefined

compact :: TableRegionName -> HBaseConnection -> IO()
compact = undefined

majorCompact :: TableRegionName -> HBaseConnection -> IO()
majorCompact = undefined

getTableNames :: HBaseConnection -> IO [TableName]
getTableNames = undefined

-----------Utility Functions-----------------
--convert a string to list of Word8
strToWord8s :: String -> [Word8]
strToWord8s = BSI.unpackBytes . BC.pack 

strToLazyByteString::String -> BL.ByteString
strToLazyByteString = BL.pack.strToWord8s

maybeStrToText:: Maybe String -> Maybe TL.Text
maybeStrToText str = case str of
                Nothing -> Nothing
                Just s -> Just $ TL.pack s
                
maybeStrToLazyByteString:: Maybe String -> Maybe BL.ByteString 
maybeStrToLazyByteString str = case str of
                                Nothing -> Nothing
                                Just s -> Just $ strToLazyByteString s 

fColDescFromHColDesc::HBaseColumnDescriptor -> ColumnDescriptor
fColDescFromHColDesc d  = ColumnDescriptor 
  {
      f_ColumnDescriptor_name                   = maybeStrToLazyByteString  $ columnName d
    , f_ColumnDescriptor_maxVersions            = columnMaxVersions d
    , f_ColumnDescriptor_compression            = maybeStrToText $ columnCompression d
    , f_ColumnDescriptor_inMemory               = columnInMemory d
    , f_ColumnDescriptor_bloomFilterType        = maybeStrToText $ columnBloomFilterType d
    , f_ColumnDescriptor_bloomFilterVectorSize  = columnFilterVectorSize d
    , f_ColumnDescriptor_bloomFilterNbHashes    = columnFilterNbHashes d
    , f_ColumnDescriptor_blockCacheEnabled      = columnCacheEnabled d
    , f_ColumnDescriptor_timeToLive             = columnTimeToLive d
  }

