{-# LANGUAGE OverloadedStrings #-}
{-
Client definition for Thrift API.  This API is for the Thrift1 API.  There is a separate client for the Thrift2 API
-}
module Database.HBase.Client
(
      HBaseConnectionSource(..)
    , HBaseColumnDescriptor(..)
    , HBaseConnection(..)
    , Put(..)
    , BatchPut(..)
    , Scan(..)
    , Increment(..)
    , TableName  
    , TableRegionName
    , RowKey             
    , Value             
    , ColumnName         
    , ColumnFamily       
    , TimeStamp          
    , FilterString       
    , ScanId             
    , StartAndPrefix   
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
    , putRowTs
    , putRows
    , putRowsTs
    , get
    , getRow 
    , getRows
    , getRowWithColumns
    , getRowWithColumnsTs
    , getRowsWithColumnsTs
    , getRowTs
    , getRowOrBefore
    , getRegionInfo
    , atomicIncrement
    , increment
    , incrementRows
    , deleteAll
    , deleteAllTs
    , deleteAllRow
    , deleteAllRowTs
    , scannerOpenWithScan
    , scannerClose
    , scannerOpenWithStop
    , scannerOpen
    , scannerOpenWithPrefix
    , scannerOpenTs
    , scannerOpenWithStopTs
    , scannerGetList
    , scannerGet
    
    
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
import              GHC.Int
import              Database.HBase.Internal.Thrift.Hbase_Types
import qualified    Database.HBase.Internal.Thrift.Hbase_Client as HClient
import qualified    Data.Vector                 as Vector
import qualified    Data.HashMap.Strict as HashMap

------------Data Structures-------------------
type TableName          = String  
type TableRegionName    = String  
type RowKey             = BL.ByteString
type Value              = BL.ByteString
type ColumnName         = String
type ColumnFamily       = String
type TimeStamp          = Int64
type FilterString       = String
type ScanId             = Int32
type StartAndPrefix     = BL.ByteString

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



data Put = Put
    {
          putColumnName ::ColumnName
        , putValue      ::Value
    }deriving (Show)
data BatchPut = BatchPut
    {
          batchPutRowKey    ::BL.ByteString
        , batchPuts         ::[Put]
    }    
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

data Increment = Increment
    {
          incrementTable    :: String
        , incrementRow      :: BL.ByteString
        , incrementColumn   :: String
        , incrementAmount   :: Int64
    }   
    
data Scan = Scan 
    {
          scanStartRow      :: BL.ByteString
        , scanStopRow       :: Maybe BL.ByteString
        , scanTimeStamp     :: Maybe Int64
        , scanColumns       :: [ColumnName]
        , scanCaching       :: Int32
        , scanFilterString  :: Maybe FilterString
        , scanBatchSize     :: Maybe Int32
        , scanSortColumns   :: Bool   
    }    
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

getRowWithColumnsTs :: TableName -> RowKey -> [ColumnName] -> TimeStamp -> HBaseConnection -> IO (Vector.Vector RowResult)
getRowWithColumnsTs t r c ts conn = do
    results <- HClient.getRowWithColumnsTs (connectionIpOp conn) (strToLazy t) r  (Vector.fromList $ map  strToLazy c) ts HashMap.empty
    return $ Vector.map tRowResultToRowResult results

getRowsWithColumnsTs :: TableName -> [RowKey]-> [ColumnName] -> TimeStamp -> HBaseConnection->IO (Vector.Vector RowResult)
getRowsWithColumnsTs t r c ts conn = do
    results <- HClient.getRowsWithColumnsTs (connectionIpOp conn) (strToLazy t) (Vector.fromList r) (Vector.fromList $ map  strToLazy c) ts HashMap.empty
    return $ (Vector.map tRowResultToRowResult results) 

getRows :: TableName -> [RowKey]->HBaseConnection -> IO (Vector.Vector RowResult)
getRows t r conn = do
    results <- HClient.getRows (connectionIpOp conn) (strToLazy t) (Vector.fromList r) HashMap.empty
    return $ Vector.map tRowResultToRowResult results
disableTable::TableName -> HBaseConnection -> IO()
disableTable t c= HClient.disableTable (connectionIpOp c) (strToLazy t)

getRowOrBefore::TableName -> RowKey->ColumnFamily->HBaseConnection -> IO (Vector.Vector RowResultValue)
getRowOrBefore t r f conn = do
    result <- HClient.getRowOrBefore (connectionIpOp conn) (strToLazy t) r (strToLazy f) 
    return $ Vector.map tCellToResultValue result
    
deleteTable::TableName -> HBaseConnection -> IO()
deleteTable t c = HClient.deleteTable (connectionIpOp c) (strToLazy t)

enableTable :: TableName -> HBaseConnection ->IO()
enableTable t c = HClient.enableTable (connectionIpOp c) (strToLazy t) 

compact :: TableRegionName -> HBaseConnection -> IO()
compact tr c= HClient.compact (connectionIpOp c) (strToLazy tr) 

majorCompact :: TableRegionName -> HBaseConnection -> IO()
majorCompact tr c = HClient.majorCompact (connectionIpOp c) (strToLazy tr)

getColumnDescriptors ::TableName-> HBaseConnection -> IO (HashMap.HashMap ColumnName HBaseColumnDescriptor)
getColumnDescriptors t c = 
  do
    results <- HClient.getColumnDescriptors (connectionIpOp c) (strToLazy t)
    return $ HashMap.fromList $ map  (\(n,cols) -> (lazyToString n, hColDescFromfColDesc cols)) (HashMap.toList results)


getTableRegions :: TableName -> HBaseConnection -> IO (Vector.Vector RegionInfo)
getTableRegions t c = do
    regions <- HClient.getTableRegions ( connectionIpOp c) (strToLazy t)
    return $ fmap tRegionInfoToRegionInfo regions

tRegionInfoToRegionInfo ::TRegionInfo -> RegionInfo
tRegionInfoToRegionInfo r = RegionInfo
                        {
                              startKey = f_TRegionInfo_startKey r
                            , endKey = f_TRegionInfo_endKey r
                            , regionId = f_TRegionInfo_id r
                            , regionName = fmap (lazyToString) $ f_TRegionInfo_name r
                            , regionInfoVersion = f_TRegionInfo_version r
                            , regionInfoServerName = fmap (lazyToString) $ f_TRegionInfo_serverName r
                            , regionInfoPort = f_TRegionInfo_port r
                            
                        }                       
getRegionInfo:: RowKey -> HBaseConnection -> IO RegionInfo
getRegionInfo r conn = do
    region <- HClient.getRegionInfo (connectionIpOp conn) r 
    return $ tRegionInfoToRegionInfo region
    
putRow::TableName -> RowKey->[Put]->HBaseConnection -> IO()
putRow t r p c = 
    HClient.mutateRow (connectionIpOp c) (strToLazy t) r (putsToMutations p) HashMap.empty

putRowTs :: TableName -> RowKey -> [Put] -> TimeStamp -> HBaseConnection -> IO()
putRowTs t r p ts c= 
    HClient.mutateRowTs (connectionIpOp c) (strToLazy t) r (putsToMutations p ) ts HashMap.empty

putRows :: TableName -> [BatchPut] -> HBaseConnection -> IO()
putRows t b c= 
    HClient.mutateRows (connectionIpOp c) (strToLazy t) (batchPutsToBatchMutations b) HashMap.empty

putRowsTs :: TableName -> [BatchPut] -> TimeStamp -> HBaseConnection -> IO()
putRowsTs t b ts c = 
    HClient.mutateRowsTs (connectionIpOp c) (strToLazy t) (batchPutsToBatchMutations b) ts HashMap.empty 
    
atomicIncrement::TableName -> RowKey -> ColumnName -> Int64 -> HBaseConnection -> IO Int64
atomicIncrement t r c i conn=  HClient.atomicIncrement (connectionIpOp conn) (strToLazy t) r (strToLazy c) i 
    
-- increment (ip,op) arg_increment
increment :: Increment -> HBaseConnection -> IO ()
increment i conn= 
    HClient.increment (connectionIpOp conn) (incrementToTIncrement i) 
    
-- incrementRows (ip,op) arg_increments
incrementRows :: [Increment] -> HBaseConnection -> IO ()
incrementRows i conn = 
    HClient.incrementRows (connectionIpOp conn) (Vector.fromList $ map incrementToTIncrement i)
    
deleteAll :: TableName-> RowKey -> ColumnName -> HBaseConnection -> IO()
deleteAll t r c conn = HClient.deleteAll (connectionIpOp conn) (strToLazy t) r (strToLazy c) HashMap.empty

deleteAllTs:: TableName -> RowKey -> ColumnName -> TimeStamp -> HBaseConnection -> IO()
deleteAllTs t r c ts conn = HClient.deleteAllTs (connectionIpOp conn) (strToLazy t) r (strToLazy c) ts HashMap.empty

deleteAllRow::TableName -> RowKey -> HBaseConnection->IO()
deleteAllRow t r conn= HClient.deleteAllRow (connectionIpOp conn) (strToLazy t) r HashMap.empty

deleteAllRowTs::TableName -> RowKey -> TimeStamp->HBaseConnection -> IO()
deleteAllRowTs t r ts conn = HClient.deleteAllRowTs (connectionIpOp conn) (strToLazy t) r ts HashMap.empty

scannerOpenWithScan :: TableName -> Scan -> HBaseConnection -> IO ScanId
scannerOpenWithScan t s conn = 
    HClient.scannerOpenWithScan (connectionIpOp conn) (strToLazy t) (scanToTScan s) HashMap.empty

scannerOpenWithStop::TableName->RowKey->RowKey->[ColumnName]->HBaseConnection->IO ScanId
scannerOpenWithStop t start stop cols conn = 
    HClient.scannerOpenWithStop (connectionIpOp conn) (strToLazy t) start stop (Vector.fromList $ map strToLazy cols) HashMap.empty

scannerOpen::TableName->RowKey->[ColumnName]->HBaseConnection -> IO ScanId
scannerOpen t start cols conn = 
    HClient.scannerOpen (connectionIpOp conn) (strToLazy t) start (Vector.fromList $ map strToLazy cols) HashMap.empty

scannerOpenWithPrefix::TableName->StartAndPrefix->[ColumnName] ->HBaseConnection -> IO ScanId
scannerOpenWithPrefix t s cols conn = 
    HClient.scannerOpenWithPrefix (connectionIpOp conn) (strToLazy t) s (Vector.fromList $ map strToLazy cols) HashMap.empty
scannerOpenTs ::TableName->RowKey->[ColumnName]->TimeStamp -> HBaseConnection-> IO ScanId

scannerOpenTs t s cols ts conn = 
    HClient.scannerOpenTs (connectionIpOp conn) (strToLazy t) s (Vector.fromList $ map strToLazy cols) ts HashMap.empty

scannerOpenWithStopTs ::TableName -> RowKey->RowKey-> [ColumnName]->TimeStamp->HBaseConnection -> IO ScanId
scannerOpenWithStopTs t start stop cols ts conn = 
    HClient.scannerOpenWithStopTs (connectionIpOp conn) (strToLazy t) start stop (Vector.fromList $ map strToLazy cols) ts HashMap.empty
 
scannerGet ::ScanId -> HBaseConnection -> IO (Vector.Vector RowResult)
scannerGet s conn = do
    results <- HClient.scannerGet (connectionIpOp conn) s
    return $ Vector.map tRowResultToRowResult results

scannerGetList::ScanId -> Int32->HBaseConnection -> IO (Vector.Vector RowResult)
scannerGetList s nbrOfRows conn = do
    results <- HClient.scannerGetList (connectionIpOp conn) s nbrOfRows  
    return $ Vector.map tRowResultToRowResult results
    
scannerClose :: ScanId->HBaseConnection->IO()
scannerClose s conn = HClient.scannerClose (connectionIpOp conn) s


-----------Utility Functions-----------------
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

putsToMutations::[Put] -> Vector.Vector Mutation
putsToMutations puts = Vector.fromList $ map (\p -> Mutation 
                                                    {
                                                       f_Mutation_isDelete = Just False
                                                     , f_Mutation_column = Just $ strToLazy $ putColumnName p
                                                     , f_Mutation_value = Just $ putValue p
                                                     , f_Mutation_writeToWAL = Just True
                                                    }
                                            )  puts
--data BatchMutation = BatchMutation{f_BatchMutation_row :: Maybe ByteString,f_BatchMutation_mutations :: Maybe (Vector.Vector Mutation)} deriving (Show,Eq,Typeable)
                                            
batchPutsToBatchMutations :: [BatchPut] -> Vector.Vector BatchMutation
batchPutsToBatchMutations bps = Vector.fromList $ map (\bp -> BatchMutation
                                                          {
                                                              f_BatchMutation_row = Just $ batchPutRowKey bp
                                                            , f_BatchMutation_mutations = Just $ putsToMutations $ batchPuts bp
                                                            
                                                          }
                                                       )  bps

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
    
incrementToTIncrement::Increment -> TIncrement
incrementToTIncrement i = TIncrement
    {
          f_TIncrement_table = Just $ strToLazy $ incrementTable i
        , f_TIncrement_row = Just $ incrementRow i
        , f_TIncrement_column = Just $ strToLazy $ incrementColumn i
        , f_TIncrement_ammount = Just $ incrementAmount i
    }    
scanToTScan::Scan -> TScan
scanToTScan s = TScan 
    {
          f_TScan_startRow = Just $ scanStartRow s
        , f_TScan_stopRow = scanStopRow s
        , f_TScan_timestamp = scanTimeStamp s
        , f_TScan_columns = Just $ Vector.fromList $ map strToLazy $ scanColumns s
        , f_TScan_caching = Just $ scanCaching s
        , f_TScan_filterString = fmap strToLazy $ scanFilterString s
        , f_TScan_batchSize = scanBatchSize s
        , f_TScan_sortColumns = Just $ scanSortColumns s          
    }            