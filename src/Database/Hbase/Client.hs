{-# LANGUAGE OverloadedStrings #-}
module Database.Hbase.Client
(
      HBaseConnectionSource(..)
    , HBaseColumnDescriptor(..)
    , HBaseConnection(..)
    , QualifiedColumnName(..)
    , Put(..)
    , TableName
    , RowKey
    , Value
    , defaultHBaseConnectionSource
    , defaultColumnDescriptor
    , openConnection
    , closeConnection
    , createTable
    , getTableNames
    , disableTable
    , deleteTable
    , enableTable
    , getTableRegions
    , compact
    , majorCompact
    , getColumnDescriptors
    , putRow
    , get
    , getRow 
    , getRowWithColumns
    , getRowWithColumnsTs
    , getRowTs
) where

import qualified    Data.ByteString.Lazy        as BL
import qualified    Data.ByteString.Char8       as BC
import qualified    Data.ByteString.Internal    as BSI
import qualified    Data.ByteString             as BS
import qualified    Data.Text.Lazy              as TL
import              Network
import              GHC.IO.Handle.Types
import              Thrift.Transport.Handle
import              Thrift.Transport.Framed
import              Thrift.Protocol.Binary
import              GHC.Word(Word8)
import              GHC.Int
import              Database.Hbase.Internal.Hbase_Types
import qualified    Database.Hbase.Internal.Hbase_Client as HClient
import qualified    Data.Vector                 as Vector
import qualified    Data.HashMap.Strict as HashMap
import              Data.Int

------------Data Structures-------------------
type TableName          = String  
type TableRegionName    = String  
type RowKey             = BL.ByteString
type Value              = BL.ByteString
type ColumnName         = String
type TimeStamp          = Int64

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
          columnName                    :: Maybe String
        , columnMaxVersions             :: Maybe Int32
        , columnCompression             :: Maybe String
        , columnInMemory                :: Maybe Bool
        , columnBloomFilterType         :: Maybe String
        , columnBloomFilterVectorSize   :: Maybe Int32
        , columnBloomFilterNbHashes     :: Maybe Int32
        , columnBlockCacheEnabled       :: Maybe Bool
        , columnTimeToLive              :: Maybe Int32
   }deriving (Show)

data HBaseConnection = HBaseConnection
    {
          handle                :: Handle
        , connectionSource      :: HBaseConnectionSource  
        , connectionProtocol    :: BinaryProtocol Handle
        , connectionIpOp        :: (BinaryProtocol Handle, BinaryProtocol Handle)
    }

defaultColumnDescriptor :: HBaseColumnDescriptor     
defaultColumnDescriptor=HBaseColumnDescriptor 
    {
          columnName                    = Nothing
        , columnMaxVersions             = Nothing
        , columnCompression             = Nothing
        , columnInMemory                = Nothing
        , columnBloomFilterType         = Nothing
        , columnBloomFilterVectorSize   = Nothing
        , columnBloomFilterNbHashes     = Nothing
        , columnBlockCacheEnabled       = Nothing
        , columnTimeToLive              = Nothing
    }       

data QualifiedColumnName = QualifiedColumnName 
    {
          qualifiedColumnFamily :: String
        , qualifiedColumnName   :: String
    }deriving (Show)

data Put = Put
    {
          qualifiedColumn      ::QualifiedColumnName
        , value                ::Value
    }deriving (Show)
    
data RowResultValue = RowResultValue
    {
          rowResultValue        :: Maybe BL.ByteString
        , rowResultTimeStamp    :: Maybe Int64
    }deriving (Show) 
    
data RowResultColumn = RowResultColumn
    {
          rowResultColumn       :: Maybe String
        , rowResultColumnValue  :: Maybe RowResultValue
    }deriving (Show)

data RowResult = RowResult 
    {
         rowResultKey           :: Maybe RowKey
       , rowResultColumns       :: Maybe (HashMap.HashMap ColumnName RowResultValue)
       , rowResultSortedColumns :: Maybe (Vector.Vector RowResultColumn)
    }deriving (Show)

data RegionInfo = RegionInfo
    {
          startKey              :: Maybe BL.ByteString
        , endKey                :: Maybe BL.ByteString
        , regionId              :: Maybe Int64
        , regionName            :: Maybe String
        , regionInfoVersion     :: Maybe Int8
        , regionInfoServerName  :: Maybe String
        , regionInfoPort        :: Maybe Int32
    }deriving (Show)
   
------------Functions-------------------------

openConnection :: HBaseConnectionSource -> IO HBaseConnection
openConnection c = do
    h <- hOpen (hostName c, port c)
    return $ HBaseConnection{ connectionSource = c
                            , handle = h
                            , connectionProtocol = BinaryProtocol h
                            , connectionIpOp = (BinaryProtocol h, BinaryProtocol h)
    }

closeConnection :: HBaseConnection -> IO()
closeConnection c = tClose $ handle c
    
createTable :: TableName->[HBaseColumnDescriptor]->HBaseConnection-> IO()
createTable t l c =   
    HClient.createTable (connectionProtocol c,connectionProtocol c) 
                        (strToLazy t) 
                        (Vector.fromList $ map fColDescFromHColDesc l)

getTableNames :: HBaseConnection -> IO [TableName]
getTableNames conn = do
        vector <- HClient.getTableNames (connectionIpOp conn)
        return $ map (BC.unpack . lazyToStrict) $ Vector.toList vector

putRow::TableName -> RowKey->[Put]->HBaseConnection -> IO()
putRow t r p c = do
        HClient.mutateRow (connectionIpOp c) (strToLazy t) r (putsToMutations p) HashMap.empty

get :: TableName -> RowKey -> ColumnName -> HBaseConnection-> IO (Vector.Vector RowResultValue)
get t r c conn= do
    result <- HClient.get (connectionIpOp conn) (strToLazy t) r (strToLazy c) HashMap.empty
    return $ Vector.map tCellToResultValue result
    
getRow::TableName->RowKey->HBaseConnection->IO (Vector.Vector RowResult)
getRow t r conn = do
    results <-HClient.getRow (connectionIpOp conn) (strToLazy t) r HashMap.empty
    return $ Vector.map tRowResultToRowResult results

getRowWithColumns::TableName->RowKey->[ColumnName]->HBaseConnection -> IO (Vector.Vector RowResult)
getRowWithColumns t r cols conn = do
        let columns = Vector.fromList $ map strToLazy cols
        results <-HClient.getRowWithColumns (connectionIpOp conn) (strToLazy t) r columns HashMap.empty
        return $ Vector.map tRowResultToRowResult results

getRowTs::TableName -> RowKey ->  TimeStamp -> HBaseConnection -> IO (Vector.Vector RowResult)
getRowTs t r ts conn= do
    results <- HClient.getRowTs (connectionIpOp conn) (strToLazy t) r ts HashMap.empty 
    return $ Vector.map tRowResultToRowResult results

--getRowWithColumnsTs (ip,op) arg_tableName arg_row arg_columns arg_timestamp arg_attributes
getRowWithColumnsTs :: TableName -> RowKey -> [ColumnName] -> TimeStamp -> HBaseConnection -> IO (Vector.Vector RowResult)
getRowWithColumnsTs t r c ts conn = do
    results <- HClient.getRowWithColumnsTs (connectionIpOp conn) (strToLazy t) r  (Vector.fromList $ map  strToLazy c) ts HashMap.empty
    return $ Vector.map tRowResultToRowResult results

disableTable::TableName -> HBaseConnection -> IO()
disableTable t c= HClient.disableTable (connectionIpOp c) (strToLazy t)

deleteTable::TableName -> HBaseConnection -> IO()
deleteTable t c = HClient.deleteTable (connectionIpOp c) (strToLazy t)

enableTable :: TableName -> HBaseConnection ->IO()
enableTable t c = HClient.enableTable (connectionIpOp c) (strToLazy t) 

compact :: TableRegionName -> HBaseConnection -> IO()
compact tr c= HClient.compact (connectionIpOp c) (strToLazy tr) 

majorCompact :: TableRegionName -> HBaseConnection -> IO()
majorCompact tr c = HClient.majorCompact (connectionIpOp c) (strToLazy tr)

getColumnDescriptors ::TableName-> HBaseConnection -> IO (HashMap.HashMap ColumnName HBaseColumnDescriptor)
getColumnDescriptors t c = do
    results <- HClient.getColumnDescriptors (connectionIpOp c) (strToLazy t)
    return $    HashMap.fromList $ map  (\(n,cols) -> (lazyToString n, hColDescFromfColDesc cols)) (HashMap.toList results)


getTableRegions :: TableName -> HBaseConnection -> IO (Vector.Vector RegionInfo)
getTableRegions t c = do
    regions <- HClient.getTableRegions ( connectionIpOp c) (strToLazy t)
    return $ fmap (\r -> RegionInfo
                        {
                              startKey = f_TRegionInfo_startKey r
                            , endKey = f_TRegionInfo_endKey r
                            , regionId = f_TRegionInfo_id r
                            , regionName = fmap (lazyToString) $ f_TRegionInfo_name r
                            , regionInfoVersion = f_TRegionInfo_version r
                            , regionInfoServerName = fmap (lazyToString) $ f_TRegionInfo_serverName r
                            , regionInfoPort = f_TRegionInfo_port r
                            
                        }) regions
-----------Utility Functions-----------------
--convert a string to list of Word8
strToWord8s :: String -> [Word8]
strToWord8s = BSI.unpackBytes . BC.pack 

strToLazy::String -> BL.ByteString
strToLazy = BL.pack.strToWord8s

lazyToStrict :: BL.ByteString -> BS.ByteString
lazyToStrict lazy = BS.concat $ BL.toChunks lazy

lazyToString:: BL.ByteString -> String
lazyToString lazy = BC.unpack . BS.concat $ BL.toChunks lazy

fColDescFromHColDesc::HBaseColumnDescriptor -> ColumnDescriptor
fColDescFromHColDesc d  = ColumnDescriptor 
  {
      f_ColumnDescriptor_name                   = fmap strToLazy $ columnName d
    , f_ColumnDescriptor_maxVersions            = columnMaxVersions d
    , f_ColumnDescriptor_compression            = fmap TL.pack $ columnCompression d
    , f_ColumnDescriptor_inMemory               = columnInMemory d
    , f_ColumnDescriptor_bloomFilterType        = fmap TL.pack $ columnBloomFilterType d
    , f_ColumnDescriptor_bloomFilterVectorSize  = columnBloomFilterVectorSize d
    , f_ColumnDescriptor_bloomFilterNbHashes    = columnBloomFilterNbHashes d
    , f_ColumnDescriptor_blockCacheEnabled      = columnBlockCacheEnabled d
    , f_ColumnDescriptor_timeToLive             = columnTimeToLive d
  }
  
hColDescFromfColDesc::ColumnDescriptor -> HBaseColumnDescriptor
hColDescFromfColDesc d = HBaseColumnDescriptor
    {
          columnName                    = fmap lazyToString $ f_ColumnDescriptor_name d
        , columnMaxVersions             = f_ColumnDescriptor_maxVersions d
        , columnCompression             = fmap TL.unpack $ f_ColumnDescriptor_compression d
        , columnInMemory                = f_ColumnDescriptor_inMemory d
        , columnBloomFilterType         = fmap TL.unpack $ f_ColumnDescriptor_bloomFilterType d
        , columnBloomFilterVectorSize   = f_ColumnDescriptor_bloomFilterVectorSize d
        , columnBloomFilterNbHashes     = f_ColumnDescriptor_bloomFilterNbHashes d
        , columnBlockCacheEnabled       = f_ColumnDescriptor_blockCacheEnabled d
        , columnTimeToLive              = f_ColumnDescriptor_timeToLive d
        
    }  
convertQualifiedColumn::QualifiedColumnName-> BL.ByteString
convertQualifiedColumn c = strToLazy $ qualifiedColumnFamily c ++ ":" ++ qualifiedColumnName c

putsToMutations::[Put] -> Vector.Vector Mutation
putsToMutations puts = Vector.fromList $ map (\p -> Mutation 
                                                    {
                                                       f_Mutation_isDelete = Just False
                                                     , f_Mutation_column = Just $ convertQualifiedColumn $ qualifiedColumn p
                                                     , f_Mutation_value = Just $ value p
                                                     , f_Mutation_writeToWAL = Just True
                                                    }
                                            )  puts

tCellToResultValue::TCell->RowResultValue
tCellToResultValue t = RowResultValue 
    {
        rowResultValue = f_TCell_value t
      , rowResultTimeStamp = f_TCell_timestamp t
    }
tColumnToRowResultColumn::TColumn->RowResultColumn
tColumnToRowResultColumn t =RowResultColumn 
    {
           rowResultColumn = fmap (BC.unpack . lazyToStrict) $ f_TColumn_columnName t
         , rowResultColumnValue = fmap tCellToResultValue $ f_TColumn_cell t
    }  
    
convertResultColumns:: (HashMap.HashMap BL.ByteString TCell) -> (HashMap.HashMap String RowResultValue)   
convertResultColumns t = HashMap.fromList . map (\(bs,tcell) -> (BC.unpack $ lazyToStrict bs,tCellToResultValue tcell )) $ HashMap.toList t

tRowResultToRowResult::TRowResult -> RowResult
tRowResultToRowResult t = RowResult
    {
          rowResultKey = f_TRowResult_row t    
        , rowResultColumns = fmap convertResultColumns $ f_TRowResult_columns t
        , rowResultSortedColumns = fmap ( Vector.map tColumnToRowResultColumn) $ f_TRowResult_sortedColumns t
    }                