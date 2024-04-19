package org.apache.doris.load;

public class TransactionManager {

    public long beginTxn() {
        return -1L;
    }

    public void commitTxn(long txnId) {

    }

    public void abortTxn(long txnId) {

    }

}
