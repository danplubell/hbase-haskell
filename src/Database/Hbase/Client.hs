{-# LANGUAGE OverloadedStrings #-}
module Database.Hbase.Client
(
    HBaseConnectionSource(..),
    HBaseColumnDescriptor(..),
    HBaseConnection(..),
    QualifiedColumnName(..),
    Put(..),
    defaultHBaseConnectionSource,
    defaultColumnDescriptor,
    openConnection,
    closeConnection,
    createTable,
    getTableNames,
    disableTable,
    deleteTable,
    putRow 
) where

import qualified    Data.ByteString.Lazy        as BL
import qualified    Data.ByteString.Char8       as BC
import qualified    Data.ByteString.Internal    as BSI
import qualified    Data.ByteString             as BS
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
import qualified    Data.HashMap.Strict as HashMap

------------Data Structures-------------------
type TableName = String  
type TableRegionName = String  
type RowKey = BL.ByteString
type Value = BL.ByteString

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
data QualifiedColumnName = QualifiedColumnName 
    {
          qualifiedColumnFamily :: String
        , qualifiedColumnName   :: String
    }deriving (Show)
data Put = Put
    {
          qualifiedColumn::QualifiedColumnName
        , value::Value
    }deriving (Show)
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
                        (strToLazy t) 
                        (Vector.fromList $ map fColDescFromHColDesc l)


getTableNames :: HBaseConnection -> IO [TableName]
getTableNames conn = do
        vector <- HClient.getTableNames (connectionProtocol conn, connectionProtocol conn)
        return $ map (BC.unpack . lazyToStrict) $ Vector.toList vector

--mutateRow (ip,op) arg_tableName arg_row arg_mutations arg_attributes
putRow::TableName -> RowKey->[Put]->HBaseConnection -> IO()
putRow t r p c = do
        HClient.mutateRow (connectionProtocol c, connectionProtocol c) (strToLazy t) r (putsToMutations p) HashMap.empty

disableTable::TableName -> HBaseConnection -> IO()
disableTable t c= HClient.disableTable (connectionProtocol c, connectionProtocol c) (strToLazy t)

deleteTable::TableName -> HBaseConnection -> IO()
deleteTable t c = HClient.deleteTable (connectionProtocol c, connectionProtocol c) (strToLazy t)

enableTable :: TableName -> HBaseConnection ->IO()
enableTable = undefined

compact :: TableRegionName -> HBaseConnection -> IO()
compact = undefined

majorCompact :: TableRegionName -> HBaseConnection -> IO()
majorCompact = undefined


-----------Utility Functions-----------------
--convert a string to list of Word8
strToWord8s :: String -> [Word8]
strToWord8s = BSI.unpackBytes . BC.pack 

strToLazy::String -> BL.ByteString
strToLazy = BL.pack.strToWord8s

lazyToStrict :: BL.ByteString -> BS.ByteString
lazyToStrict lazy = BS.concat $ BL.toChunks lazy

maybeStrToText:: Maybe String -> Maybe TL.Text
maybeStrToText str = case str of
                Nothing -> Nothing
                Just s -> Just $ TL.pack s
                
maybeStrToLazyByteString:: Maybe String -> Maybe BL.ByteString 
maybeStrToLazyByteString str = case str of
                                Nothing -> Nothing
                                Just s -> Just $ strToLazy s 

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
convertQualifiedColumn::QualifiedColumnName-> BL.ByteString
convertQualifiedColumn c = strToLazy $ qualifiedColumnFamily c ++ ":" ++ qualifiedColumnName c

putsToMutations::[Put] -> Vector.Vector Mutation
putsToMutations puts = Vector.fromList $ map (\p -> Mutation {
                                                               f_Mutation_isDelete = Just False
                                                             , f_Mutation_column = Just $ convertQualifiedColumn $ qualifiedColumn p
                                                             , f_Mutation_value = Just $ value p
                                                             , f_Mutation_writeToWAL = Just True
                                                            }
                                            )  puts

                                       