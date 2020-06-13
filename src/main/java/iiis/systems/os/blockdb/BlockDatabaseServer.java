package iiis.systems.os.blockdb;

import iiis.systems.os.blockchaindb.*;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

public class BlockDatabaseServer {
    private Server server;

    private void start(String address, int port) throws IOException {
        server = NettyServerBuilder.forAddress(new InetSocketAddress(address, port))
                .addService(new BlockChainMinerImpl())
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BlockDatabaseServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static boolean removeAll(String path) {
        File file = new File(path);
        if(!file.exists()){//判断是否待删除目录是否存在
            System.err.println("The dir are not exists!");
            return false;
        }

        String[] content = file.list();//取得当前目录下所有文件和文件夹
        for(String name : content){
            File temp = new File(path, name);
            if(temp.isDirectory()){//判断是否是目录
                removeAll(temp.getAbsolutePath());//递归调用，删除目录里的内容
                temp.delete();//删除空目录
            }else{
                if(!temp.delete()){//直接删除文件
                    System.err.println("Failed to delete " + name);
                }
            }
        }
        return true;
    }
    public static String randomHexString(int len) {
        try {
            StringBuffer result = new StringBuffer();
            for (int i = 0; i < len; i++) {
                result.append(Integer.toHexString(new Random().nextInt(16)));
            }
            return result.toString();

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();

        }
        return null;
    }
    public static void copyDir(String sourcePath, String newPath) throws IOException {
        File file = new File(sourcePath);
        String[] filePath = file.list();

        if (!(new File(newPath)).exists()) {
            (new File(newPath)).mkdir();
        }

        for (int i = 0; i < filePath.length; i++) {
           if (new File(sourcePath + File.separator + filePath[i]).isFile())
           {
               copyFile(sourcePath + File.separator + filePath[i], newPath + File.separator + filePath[i]);
           }

        }
    }
    public static void copyFile(String oldPath, String newPath) throws IOException {
        File oldFile = new File(oldPath);
        File file = new File(newPath);
        FileInputStream in = new FileInputStream(oldFile);
        FileOutputStream out = new FileOutputStream(file);;

        byte[] buffer=new byte[2097152];
        int readByte = 0;
        while((readByte = in.read(buffer)) != -1){
            out.write(buffer, 0, readByte);
        }

        in.close();
        out.close();
    }
    public static void main(String[] args) throws IOException, JSONException, InterruptedException {
        if (args.length < 1){
            System.out.println("Invalid command, please give server id");
            return ;
        }
        if (args[0].contains("--id=")) { //Server run
            int serverId = Integer.parseInt(args[0].substring(5));
            JSONObject serverConfig = Util.readJsonFile("config.json");
            int serverNum = serverConfig.getInt("nservers");
            JSONObject config = (JSONObject) serverConfig.get(String.format("%d", serverId));
            String address = config.getString("ip");
            int port = Integer.parseInt(config.getString("port"));
            String dataDir = config.getString("dataDir");
            removeAll("./" + dataDir);
            for (int i = 1; i <= serverNum; i++) {
                if (i != serverId) {
                    String remoteDataDir = ((JSONObject) serverConfig.get(String.format("%d", i))).getString("dataDir");
                    try {
                        copyDir("./" + remoteDataDir, "./" + dataDir);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                    break;
                }
            }
            System.out.println("Finish remote copying") ;
            DatabaseEngine.setup(dataDir, serverId);
            System.out.println("Finish enginn init") ;
            final BlockDatabaseServer server = new BlockDatabaseServer();
            server.start(address, port);
            server.blockUntilShutdown();
        }
        else {
            boolean finishTask = false ;
            for (int i = 1; i <= 3 && !finishTask; i ++) {

                JSONObject serverConfig = Util.readJsonFile("config.json");
                int serverNum = serverConfig.getInt("nservers");
                JSONObject config = (JSONObject) serverConfig.get(String.format("%d", i));
                String address = config.getString("ip");
                int port = Integer.parseInt(config.getString("port"));
                BlockDatabaseClient client = new BlockDatabaseClient(address, port) ;
                switch (args[0]) {
                    case "get":
                        if (args.length != 2) System.out.println("Invalid arguments");
                        finishTask = client.get(args[1]);
                        break;
                    case "transfer":
                        // get fromID, toID, value, miningFee
                        if (args.length != 5) System.out.println("Invalid arguments") ;
                        String uuid = randomHexString(16) ;
                        Transaction newTrans = Transaction.newBuilder().setFromID(args[1]).setToID(args[2])
                                .setValue(Integer.parseInt(args[3]))
                                .setMiningFee(Integer.parseInt(args[4]))
                                .setUUID(uuid).setType(Transaction.Types.TRANSFER).build();
                        finishTask = client.transfer(newTrans);
                        break;
                    case "verify":
                        // get fromID, toID, value, miningFee, UUID
                        if (args.length != 6) System.out.println("Invalid arguments") ;
                        Transaction verifyTrans = Transaction.newBuilder().setFromID(args[1]).setToID(args[2])
                                .setValue(Integer.parseInt(args[3]))
                                .setMiningFee(Integer.parseInt(args[4]))
                                .setUUID(args[5]).setType(Transaction.Types.TRANSFER).build();
                        finishTask = client.verify(verifyTrans);
                        break;
                    case "getHeight" :
                        if (args.length != 1) System.out.println("Invalid arguments") ;
                        finishTask = client.getHeight();
                        break ;
                    case "getBlock":
                        if (args.length != 2) System.out.println("Invalid arguments") ;
                        GetBlockRequest getBlockRequest = GetBlockRequest.newBuilder().setBlockHash(args[1]).build() ;
                        finishTask = client.getBlock(getBlockRequest);
                        break;
                }
            }
        }
    }

    static class BlockChainMinerImpl extends BlockChainMinerGrpc.BlockChainMinerImplBase {
        private final DatabaseEngine dbEngine = DatabaseEngine.getInstance();

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            int value = dbEngine.get(request.getUserID());
            GetResponse response = GetResponse.newBuilder().setValue(value).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void transfer(Transaction request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.transfer(request);
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void verify(Transaction request, StreamObserver<VerifyResponse> responseObserver) {
            VerifyResponse response = dbEngine.verify(request) ;
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getHeight(Null request, StreamObserver<GetHeightResponse> responseObserver) {
            GetHeightResponse response = dbEngine.getHeight() ;
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getBlock(GetBlockRequest request, StreamObserver<JsonBlockString> responseObserver) {
            JsonBlockString response = dbEngine.getBlock(request) ;
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void pushBlock(JsonBlockString request, StreamObserver<Null> responseObserver) {
            Null response = dbEngine.pushBlock(request) ;
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void pushTransaction(Transaction request, StreamObserver<Null> responseObserver) {
            Null response = dbEngine.pushTransaction(request) ;
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


    }
}
