package iiis.systems.os.blockdb;

import com.google.common.base.Verify;
import iiis.systems.os.blockchaindb.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
Problem:
1. JSONObject Order;
2. Datadir;

 */




public class DatabaseEngine {
    private static DatabaseEngine instance = null;
    private String serverID = "Server";

    public Lock lock = new ReentrantLock();

    public static int N = 50;

    public static int curBlockID = 0 ;

    public static DatabaseEngine getInstance() {
        return instance;
    }

    public static void setup(String dataDir, int Id) {
        instance = new DatabaseEngine(dataDir, Id);
    }

    private HashMap<Integer, HashMap<String, Integer>> balances = new HashMap<>() ;
    private HashMap<String, Block> hashToBlock = new HashMap<>() ;
    private HashMap<Integer, Block> idToBlock = new HashMap<>() ;
    private HashMap<Integer, Integer> depths = new HashMap<>() ;
    private HashMap<Integer, HashSet<String>> visitedTransactions = new HashMap<>() ;
    private Vector<Transaction> cachedTransactions = new Vector<>() ;
    private HashSet<Integer> blocksInLongestChain = new HashSet<>() ;
    private Vector<Transaction> waitingTransactions = new Vector<>() ;

    private String initHash = "" ;

    private Block minerBlock = Block.newBuilder().setBlockID(-1).build();

    public static String getHashString(String string) {

        byte[] bytes = getHashBytes(string);
        StringBuilder hexString = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }

        return hexString.toString();
    }

    public static byte[] getHashBytes(String string) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
        byte[] bytes = md.digest(string.getBytes(StandardCharsets.UTF_8));
        return bytes;
    }

    public static boolean checkHash(String Hash) {
        return Hash.substring(0,5).equals("00000");
    }
    private String dataDir;
    public int transactionID = 0;

    public String blockToString(Block block) {
        String tmp = block.getNonce() + block.getMinerID() + block.getPrevHash() ;
        List<Transaction> transList = block.getTransactionsList() ;
        for (int i = 0; i < transList.size(); i ++) {
            tmp = tmp + transToStr(transList.get(i)) ;
        }
        return tmp;
    }

    public String transToStr(Transaction cur) {
        return cur.getUUID() + cur.getMiningFee() + cur.getFromID() + cur.getToID() + cur.getValue() + cur.getType() ;
    }
    public String blockToHash(Block block) {
        return getHashString(blockToString(block)) ;
    }
    public Transaction jsonToTransaction(org.json.JSONObject jsonObject) {
        Transaction.Types tmpType = Transaction.Types.TRANSFER ;
        Transaction result = Transaction.newBuilder().setFromID(jsonObject.getString("FromID")).setToID(jsonObject.getString("ToID"))
                .setType(tmpType).setMiningFee(jsonObject.getInt("MiningFee")).setUUID(jsonObject.getString("UUID"))
                .setValue(jsonObject.getInt("Value")).build();
        return result ;
    }

    public JSONObject transactionToJson(Transaction trans) {
        JSONObject json = new JSONObject() ;
        json.put("UUID", trans.getUUID());
        json.put("Type", "TRANSFER");
        json.put("FromID", trans.getFromID());
        json.put("ToID", trans.getToID());
        json.put("Value", trans.getValue());
        json.put("MiningFee", trans.getMiningFee()) ;
        return json ;
    }

    public Block jsonToBlock(org.json.JSONObject jsonObject) {
        Block.Builder result = Block.newBuilder().setBlockID(jsonObject.getInt("BlockID")).setMinerID(jsonObject.getString("MinerID"))
                .setNonce(jsonObject.getString("Nonce")).setPrevHash(jsonObject.getString("PrevHash")) ;
        org.json.JSONArray transactions = jsonObject.getJSONArray("Transactions") ;
        for (int i = 0; i < transactions.length(); i ++) {
            result.addTransactions(jsonToTransaction(transactions.getJSONObject(i)));
        }
        return result.build() ;
    }

    public JsonBlockString blockToJsonBlockString(Block block) {

        org.json.JSONObject jsonFile = new org.json.JSONObject() ;
        jsonFile.put("BlockID", block.getBlockID()) ;
        jsonFile.put("MinerID", block.getMinerID()) ;
        jsonFile.put("Nonce", block.getNonce()) ;
        jsonFile.put("PrevHash", block.getPrevHash()) ;
        JSONArray transactions = new JSONArray();

        jsonFile.put("Transactions", new JSONArray()) ;
        List<Transaction> transList = block.getTransactionsList() ;
        for (int i = 0 ; i < transList.size(); i ++) {
            jsonFile.accumulate("Transactions", transactionToJson(transList.get(i))) ;
        }
        return JsonBlockString.newBuilder().setJson(jsonFile.toString()).build() ;
    }

    public boolean isRoot(String hash) {
        int len = hash.length();
        for (int i = 0; i < len; i ++) {
            if (!hash.startsWith("0", i)) return false ;
        }
        return true ;
     }

     public boolean verifySingleTransaction(Transaction i) {
        if (i.getMiningFee() <= 0) return false;
        return i.getValue() > i.getMiningFee() ;
     }

     public boolean checkMinerID(String id) {
         JSONObject serverConfig = new JSONObject( );
         try {
             serverConfig = Util.readJsonFile("config.json");
         } catch (IOException e ){
             e.printStackTrace();
             return false ;
         }
         int serverNum = serverConfig.getInt("nservers") ;

         for (int i = 1; i <= serverNum; i ++) {
             if (String.format("Server%02d", i).equals(id)) return true ;
         }
         return false ;
     }
     public boolean verifyBlock(Block block) {
        //Check Hash
        // System.out.println("Verifiing hash" + blockToHash(block)) ;
       //  System.out.println("With String = " + blockToString(block)) ;
        if (!checkHash(blockToHash(block))) return false ;
       // System.out.println("Verified Hash") ;
         if (!checkMinerID(block.getMinerID())) return false;
        // System.out.println("Verified minerId") ;
         //Check prev in longest branch ;
        int prevId = -1 ;
        if (hashToBlock.containsKey(block.getPrevHash()))
            prevId = hashToBlock.get(block.getPrevHash()).getBlockID();
        if (!blocksInLongestChain.contains(prevId) && (!isRoot(block.getPrevHash()))) return false ;

        // System.out.println("Verified o") ;
        //Check no double pay ;
         HashMap<String, Integer> tmpBalance = new HashMap<>() ;
         HashMap<String, Integer> oldBalance = new HashMap<>() ;
         if (prevId > 0) oldBalance = balances.get(prevId) ;
         for (String key : oldBalance.keySet())
             tmpBalance.put(key, oldBalance.get(key)) ;

        List<Transaction> transactionArray = block.getTransactionsList() ;
        HashSet<String> thisSet = new HashSet<>() ;
        HashSet<String> oldVisited = new HashSet<>() ;
        if (prevId > 0) oldVisited = visitedTransactions.get(prevId) ;
        for (Transaction i : transactionArray) {
            if (!verifySingleTransaction(i)) return false ;
            if (oldVisited.contains(i.getUUID()) || thisSet.contains(i.getUUID())) return false ;
            thisSet.add(i.getUUID()) ;
            String fromID = i.getFromID() ;
            String toID = i.getToID() ;
            int value = i.getValue() ;
            int miningFee = i.getMiningFee() ;
            String serverID = block.getMinerID() ;
            if (tmpBalance.getOrDefault(fromID, 1000) - value < 0) return false ;
            tmpBalance.put(fromID, tmpBalance.getOrDefault(fromID, 1000) - value ) ;
            tmpBalance.put(toID, tmpBalance.getOrDefault(toID, 1000) + value - miningFee) ;
            tmpBalance.put(serverID, tmpBalance.getOrDefault(serverID, 1000) + miningFee) ;
        }
        return true ;
     }
     public void addTillRoot(Block curBlock) {
        while (!isRoot(curBlock.getPrevHash())) {
            blocksInLongestChain.add(curBlock.getBlockID());
            curBlock = hashToBlock.get(curBlock.getPrevHash());
        }
        blocksInLongestChain.add(curBlock.getBlockID());
     }

     public void resetWaitingTransactions() {
        waitingTransactions.clear();
        for (Transaction i : cachedTransactions) {
            if (minerBlock.getBlockID() < 0) // non block at all ;
                waitingTransactions.add(i) ;
            else if (!visitedTransactions.get(minerBlock.getBlockID()).contains(i.getUUID())) waitingTransactions.add(i) ;
        }
     }

     public void resetLongestChain() {
        int curMaxDepth = -1 ;
        for (int key : depths.keySet()) {
            if (depths.get(key) > curMaxDepth) {
                blocksInLongestChain.clear() ;
                curMaxDepth = depths.get(key) ;
                minerBlock = idToBlock.get(key) ;
                blocksInLongestChain.clear() ;
                addTillRoot(minerBlock);
            }
            else if (depths.get(key) == curMaxDepth) {
                Block curBlock = idToBlock.get(key) ;
                addTillRoot(curBlock);
                if (blockToHash(curBlock).compareTo( blockToHash(minerBlock)) < 0) minerBlock = curBlock ;
            }
        }
     }
     public void commitNewBlock(Block block) {
        ++ curBlockID ;
        block = block.toBuilder().setBlockID(curBlockID).build() ;
       // System.out.println("Wuhu!!!") ;
        commitBlockToMemory(block) ;
      //  System.out.println("Wuhu!!") ;
        commitBlockToDisk(block);
       // System.out.println("Wuhu!!!") ;
        resetLongestChain() ;
        resetWaitingTransactions();
     }

     public void commitBlockToMemory(Block curBlock) {

         hashToBlock.put(blockToHash(curBlock), curBlock) ;
         idToBlock.put(curBlock.getBlockID(), curBlock) ;
         int prevId = 0 ;
         if (!isRoot(curBlock.getPrevHash()))
             prevId = hashToBlock.get(curBlock.getPrevHash()).getBlockID() ;

         depths.put(curBlock.getBlockID(), depths.getOrDefault(prevId, 0) + 1) ;
         List<Transaction> transactionArray = curBlock.getTransactionsList() ;
         HashMap<String, Integer> tmpBalance = new HashMap<>() ;
         HashMap<String, Integer> oldBalance = new HashMap<>() ;
         if (prevId > 0) oldBalance = balances.get(prevId) ;

         for (String key : oldBalance.keySet())
             tmpBalance.put(key, oldBalance.get(key)) ;
         HashSet<String> oldVisited = new HashSet<>() ;
         if (prevId > 0) oldVisited = visitedTransactions.get(prevId) ;

         HashSet<String> tmpVisited = new HashSet<>(oldVisited);

         for (Transaction i : transactionArray) {
             String fromID = i.getFromID() ;
             String toID = i.getToID() ;
             int value = i.getValue() ;
             int miningFee = i.getMiningFee() ;
             String serverID = curBlock.getMinerID() ;
             tmpBalance.put(fromID, tmpBalance.getOrDefault(fromID, 1000) - value ) ;
             tmpBalance.put(toID, tmpBalance.getOrDefault(toID, 1000) + value - miningFee) ;
             tmpBalance.put(serverID, tmpBalance.getOrDefault(serverID, 1000) + miningFee) ;
             tmpVisited.add(i.getUUID()) ;
         }
         balances.put(curBlock.getBlockID(), tmpBalance) ;
         visitedTransactions.put(curBlock.getBlockID(), tmpVisited) ;

     }
     public void commitBlockToDisk(Block block) {
        String path = dataDir + block.getBlockID() + ".json" ;

     //    System.out.println(path) ;
        File file = new File(path) ;
         try {
             if (!file.exists()) file.createNewFile();
          //   System.out.println(blockToJsonBlockString(block)) ;
             JsonUtils.writeJsonFile(blockToJsonBlockString(block).getJson(), path);
         } catch (IOException e) {
             e.printStackTrace();
         }
     }

     public String checkPrevHash() {
        if (minerBlock.getBlockID() < 0) return initHash ;
        return blockToHash(minerBlock) ;
     }

     public boolean checkPendingProcessInChain() {
        if (minerBlock.getBlockID() < 0) return false ;
        String curHash = blockToHash(minerBlock) ;
        int steps = 6 ;
        while (!isRoot(curHash) && steps > 0) {
            if (hashToBlock.get(curHash).getTransactionsList().size() > 0) return true ;
            curHash = hashToBlock.get(curHash).getPrevHash() ;
            steps -- ;
        }
        return false ;

     }
    public void restart() {
        int n = 1;
        String path = dataDir + n + ".json";
        File file = new File(path);
        while (file.exists()){
            curBlockID = n ;
           // System.out.println(n) ;
            try{
                Block curBlock = jsonToBlock(Util.readJsonFile(path)) ;
                commitBlockToMemory(curBlock) ;
            }catch(IOException e){
                e.printStackTrace();
            }
            n ++ ;
            path = dataDir + n + ".json";
            file = new File(path);
        }
        resetLongestChain();
        // Run miner
        new Thread(new Runnable() {
            @Override
            public void run() {
                int cnt = 0 ;
                while (true) {
                    if (waitingTransactions.size() == 0 && !checkPendingProcessInChain()) {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        continue ;
                    }
                    cnt ++ ;
                   // if (cnt % 100000 == 0) System.out.println(cnt) ;
                    String nonce = Util.getRandomPassword(8) ;
                    String prevHash = checkPrevHash() ;
                    Block.Builder builder = Block.newBuilder().setBlockID(0).setMinerID(serverID).setNonce(nonce)
                            .setPrevHash(prevHash) ;
                    // get waiting transactions ;
                    int prevId = minerBlock.getBlockID() ;
                    Vector<Transaction> tmpTransactions = new Vector<>() ;
                    HashMap<String, Integer> tmpBalance = new HashMap<>() ;
                    HashMap<String, Integer> oldBalance = new HashMap<>() ;
                    if (prevId > 0) oldBalance = balances.get(prevId) ;


                    for (int i = 0; i < waitingTransactions.size() && tmpTransactions.size() < 50; i ++) {
                        Transaction curTrans = waitingTransactions.get(i) ;
                        String fromID = curTrans.getFromID() ;
                        String toID = curTrans.getToID() ;
                        int value = curTrans.getValue() ;
                        int miningFee = curTrans.getMiningFee() ;
                        if (tmpBalance.getOrDefault(fromID, 1000) - value < 0) { // invalid in longest chain
                            waitingTransactions.remove(i) ;
                            cachedTransactions.removeIf(j -> j.getUUID().equals(curTrans.getUUID()));
                            continue ;
                        }
                        tmpTransactions.add(curTrans) ;
                        tmpBalance.put(fromID, tmpBalance.getOrDefault(fromID, 1000) - value ) ;
                        tmpBalance.put(toID, tmpBalance.getOrDefault(toID, 1000) + value - miningFee) ;
                        tmpBalance.put(serverID, tmpBalance.getOrDefault(serverID, 1000) + miningFee) ;
                    }
                    for (int i = 0; i < tmpTransactions.size(); i ++) {
                        builder.addTransactions(tmpTransactions.get(i)) ;
                    }
                    Block newBlock = builder.build() ;
                    if ((!checkHash(blockToHash(newBlock))) || (!newBlock.getPrevHash().equals(checkPrevHash()))) continue ;

               //     System.out.println(blockToHash(newBlock)) ;
               //     System.out.println("with String = " + blockToString(newBlock)) ;
                 //   System.out.println("WUhu!!") ;
                    commitNewBlock(newBlock);
                    pushBlockToOthers(blockToJsonBlockString(newBlock));
                }
            }
        }).start();
    }

    public static String randomHexString(int len) {
        try {
            StringBuffer result = new StringBuffer();
            for (int i = 0; i < len; i++) {
                result.append(Integer.toHexString(new Random().nextInt(16)));
            }
            return result.toString().toUpperCase();

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();

        }
        return null;
    }
    DatabaseEngine(String dataDir, int Id)  {
        this.dataDir = "./" + dataDir;
        this.serverID = "Server" + String.format("%02d", Id);
        for (int i = 0 ; i < 64; i ++) initHash = initHash + "0" ;
        this.restart();
   /*     for (int i = 0; i < 30; i ++) {
            Transaction newTransaction = Transaction.newBuilder().setFromID(String.format("test%03d",i))
                    .setToID(String.format("test%03d", 40)).setUUID(Objects.requireNonNull(randomHexString(16))).setValue(i * 40)
                    .setMiningFee(i * 10).setType(Transaction.Types.TRANSFER).build() ;
            transfer(newTransaction) ;
        }
        try {
            Thread.sleep(10000) ;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 10; i ++) {
            Transaction newTransaction = Transaction.newBuilder().setFromID(String.format("test%03d",i))
                    .setToID(String.format("test%03d", 40)).setUUID(Objects.requireNonNull(randomHexString(16))).setValue(i * 70)
                    .setMiningFee(i * 10).setType(Transaction.Types.TRANSFER).build() ;
            transfer(newTransaction) ;

        }
        for (int i = 0; i < 41; i ++){
            System.out.println(get(String.format("test%03d", i)));
        }
        getHeight() ;
     */
    }

    public int get(String userId) {
        if (minerBlock.getBlockID() < 0) //No block at all
            return 1000 ;
        return balances.get(minerBlock.getBlockID()).getOrDefault(userId, 1000) ;
    }


    public void pushTransactionToOthers(Transaction transaction)  {
        JSONObject serverConfig = new JSONObject( );
        try {
            serverConfig = Util.readJsonFile("config.json");
        } catch (IOException e ){
            e.printStackTrace();
            return ;
        }
        int serverNum = serverConfig.getInt("nservers") ;

        for (int i = 1; i <= serverNum; i ++) {
            if (!String.format("Server%02d", i).equals(serverID))  {
                JSONObject config = (JSONObject)serverConfig.get(String.format("%d", i));
                String address = config.getString("ip");
                int port = Integer.parseInt(config.getString("port"));
                BlockDatabaseClient client = new BlockDatabaseClient(address, port) ;
                client.pushTransaction(transaction);
            }
        }
    }

    public void pushBlockToOthers(JsonBlockString jsonBlock) {
        JSONObject serverConfig = new JSONObject( );
        try {
            serverConfig = Util.readJsonFile("config.json");
        } catch (IOException e ){
            e.printStackTrace();
            return ;
        }
        int serverNum = serverConfig.getInt("nservers") ;

        for (int i = 1; i <= serverNum; i ++) {
            if (!String.format("Server%02d", i).equals(serverID))  {
                JSONObject config = (JSONObject)serverConfig.get(String.format("%d", i));
                String address = config.getString("ip");
                int port = Integer.parseInt(config.getString("port"));
                BlockDatabaseClient client = new BlockDatabaseClient(address, port) ;
                client.pushBlock(jsonBlock);
            }
        }
    }
    public boolean transfer(Transaction transaction) {
        if (!verifySingleTransaction(transaction)) return false ;
        for (Transaction i : cachedTransactions) {
            if (i.getUUID().equals(transaction.getUUID())) return true ;
        }
        cachedTransactions.add(transaction) ;
        resetWaitingTransactions();
        pushTransactionToOthers(transaction) ;
        return true ;
    }

    String jumpSteps(String curBlockHash, int steps) {
        for (int i = 0; i < steps; i ++) {
            if (!isRoot(curBlockHash)) curBlockHash = hashToBlock.get(curBlockHash).getPrevHash() ;
            else break ;
        }
        return curBlockHash ;
    }

    public String findContainingBlock(String uuid) {
        if (minerBlock.getBlockID() < 0) return "?";
        String curHash = blockToHash(minerBlock) ;
        while (!isRoot(curHash)) {
            List<Transaction> transList = hashToBlock.get(curHash).getTransactionsList() ;
            for (Transaction i : transList) {
                if (i.getUUID().equals(uuid)) return curHash ;
            }
            curHash = hashToBlock.get(curHash).getPrevHash() ;
        }
        return "?" ;
    }
    public VerifyResponse verify(Transaction transaction) {
        String uuid = transaction.getUUID() ;
        VerifyResponse.Builder builder = VerifyResponse.newBuilder().setBlockHash(findContainingBlock(transaction.getUUID())) ;
        //Check if cached
        boolean isCached = false ;
        boolean isConfirmed = false ;
        for (Transaction i : cachedTransactions) {
            if (i.getUUID().equals(uuid)) isCached = true ;
        }

        if (minerBlock.getBlockID() > 0) { //indeed has this block
            if (visitedTransactions.get(minerBlock.getBlockID()).contains(uuid)) isCached = true ;
            String jumpResult = jumpSteps(blockToHash(minerBlock), 6) ;
            if (!isRoot(jumpResult)) {
                int resultId = hashToBlock.get(jumpResult).getBlockID() ;
                if (visitedTransactions.get(resultId).contains(uuid)) isConfirmed = true ;
            }
        }
        if (isConfirmed) builder.setResult(VerifyResponse.Results.SUCCEEDED) ;
        else if (isCached) builder.setResult(VerifyResponse.Results.PENDING) ;
        else builder.setResult(VerifyResponse.Results.FAILED) ;
        return builder.build() ;
    }

    public GetHeightResponse getHeight() {
        String leafHash = "" ;
        for (int i = 0; i < 64; i ++) leafHash = leafHash + "0" ;
        if (minerBlock.getBlockID() > 0) leafHash = blockToHash(minerBlock) ;

        return GetHeightResponse.newBuilder().setHeight(depths.getOrDefault(minerBlock.getBlockID(), 0))
                .setLeafHash(leafHash).build();
    }

    public JsonBlockString getBlock(GetBlockRequest request) {
        String hash = request.getBlockHash() ;
        if (hashToBlock.containsKey(hash)) return blockToJsonBlockString(hashToBlock.get(hash));
        else return JsonBlockString.newBuilder().build() ;
    }

    public Null pushBlock(JsonBlockString request) {
        JSONObject jsonObject = new JSONObject(request.getJson());
        Block thisBlock = jsonToBlock(jsonObject) ;
     //   System.out.println("Verifing....." + thisBlock.toString());

        if (verifyBlock(thisBlock)) {
            commitNewBlock(thisBlock) ;
        }
        return Null.newBuilder().build() ;
    }

    public Null pushTransaction(Transaction request) {
        transfer(request) ;
        return Null.newBuilder().build() ;
    }
}
