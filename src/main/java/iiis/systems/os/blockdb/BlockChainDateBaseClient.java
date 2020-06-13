package iiis.systems.os.blockdb;

import iiis.systems.os.blockchaindb.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


class BlockDatabaseClient {
    private static final Logger log = Logger.getLogger(BlockDatabaseClient.class.getName());

    private final ManagedChannel channel;
    //阻塞/同步 的stub(存根)
    private final BlockChainMinerGrpc.BlockChainMinerBlockingStub blockingStub;

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */

    public BlockDatabaseClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build());
    }

    public BlockDatabaseClient(ManagedChannel channel) {
        this.channel = channel;

        blockingStub = BlockChainMinerGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    public boolean get(String userID) {
        GetRequest request = GetRequest.newBuilder().setUserID(userID).build();
        GetResponse response = null;
        try {
            response = blockingStub.get(request);
        } catch (StatusRuntimeException e) {
            System.out.println(String.format("rpc failed"));
            return false ;
        }
        System.out.println("Get: " + response.getValue());
        return true ;
    }

    public boolean transfer(Transaction request) {

        BooleanResponse response = null;
        try {
            response = blockingStub.transfer(request) ;
        } catch (StatusRuntimeException e) {
            System.out.println("rpc failed") ;
            return false;
        }
        if (response.getSuccess()) System.out.println("Transfer Success" );
        else System.out.println("Transfer Fail, maybe you give too much money to miner") ;
        return true;
    }

    public boolean verify(Transaction request) {
        VerifyResponse response = null;
        try {
            response = blockingStub.verify(request) ;
        } catch (StatusRuntimeException e) {
            System.out.println(String.format("rpc failed:%s", e.getStatus()));
            return false ;
        }
        System.out.println(response.getBlockHash()) ;
        if (response.getResult() == VerifyResponse.Results.FAILED) System.out.println("Transfer FAILED" );
        else if (response.getResult() == VerifyResponse.Results.PENDING) System.out.println("Transfer PENDING") ;
        else System.out.println("Transfer SUCCESS") ;
        return true;
    }

    public boolean getHeight() {
        GetHeightResponse response = null ;
        try {
            response = blockingStub.getHeight(Null.newBuilder().build());
        } catch (StatusRuntimeException e) {
            System.out.println(String.format("rpc failed:%s", e.getStatus()));
            return false ;
        }
        System.out.println(response.getLeafHash() + "     " + response.getHeight()) ;
        return true ;
    }

    public boolean getBlock(GetBlockRequest request) {
        JsonBlockString response = null ;
        try {
            response = blockingStub.getBlock(request) ;
        } catch (StatusRuntimeException e) {
            System.out.println(String.format("rpc failed:%s", e.getStatus()));
            return false;
        }
        System.out.println(response.getJson()) ;
        return true ;
    }

    public void pushTransaction(Transaction request) {
        Null response = null ;
        try {
            response = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS).pushTransaction(request) ;
        } catch (StatusRuntimeException e) {
            System.out.println(String.format("rpc failed:%s", e.getStatus()));
            return ;
        }
    }

    public void pushBlock(JsonBlockString request) {
        Null response = null ;
        try {
            response = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS).pushBlock(request) ;
        } catch (StatusRuntimeException e) {
            System.out.println(String.format("rpc failed:%s", e.getStatus()));
            return ;
        }
    }
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
}