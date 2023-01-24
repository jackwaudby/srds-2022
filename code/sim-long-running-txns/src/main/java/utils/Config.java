package utils;

public class Config {
    private static final Config instance = new Config();

    private static int clusterSize;
    private static double epochTimeout;
    private static double commitOperationRate;
    private static double abortOperationRate;
    private static double transactionServiceRate;
    private static double propDistributedTransactions;
    private static double propLongRunningTransactions;
    private static final long seedValue = 0;
    private static boolean fixSeed = true;
    private static boolean fixedEpochTimeout = true;

    private Config() {

    }

    public static Config getInstance() {
        return instance;
    }

    public boolean isFixedEpochTimeout() {
        return fixedEpochTimeout;
    }

    public void setFixedEpochTimeout(boolean fixedEpochTimeout) {
        Config.fixedEpochTimeout = fixedEpochTimeout;
    }

    public void setClusterSize(int clusterSize) {
        Config.clusterSize = clusterSize;
    }

    public int getClusterSize() {
        return clusterSize;
    }

    public double getEpochTimeoutInMillis() {
        return epochTimeout;
    }

    public double getEpochTimeoutInSecs() {
        return epochTimeout / 1000.0;
    }

    public double getTransactionServiceRateInMillis() {
        return transactionServiceRate;
    }

    public double getTransactionServiceRateInSecs() {
        return transactionServiceRate / 1000.0;
    }

    public double getCommitOperationRateInMillis() {
        return commitOperationRate;
    }

    public double getCommitOperationRateInSecs() {
        return commitOperationRate / 1000.0;
    }

    public double getAbortOperationRateInMillis() {
        return abortOperationRate;
    }

    public double getAbortOperationRateInSecs() {
        return abortOperationRate / 1000.0;
    }

    public long getSeedValue() {
        return seedValue;
    }

    public boolean isSeedSet() {
        return fixSeed;
    }

    public void setEpochTimeout(double epochTimeout) {
        Config.epochTimeout = epochTimeout;
    }

    public void setCommitOperationRate(double twoPhaseCommitDelay) {
        Config.commitOperationRate = twoPhaseCommitDelay;
    }

    public void setAbortOperationRate(double abortOperationRate) {
        Config.abortOperationRate = abortOperationRate;
    }

    public void setTransactionServiceRate(double transactionServiceRate) {
        Config.transactionServiceRate = transactionServiceRate;
    }

    public void setFixSeed(boolean fixSeed) {
        Config.fixSeed = fixSeed;
    }

    public double getPropDistributedTransactions() {
        return propDistributedTransactions;
    }

    public void setPropDistributedTransactions(double propDistributedTransactions) {
        Config.propDistributedTransactions = propDistributedTransactions;
    }

    public double getPropLongRunningTransactions() {
        return propLongRunningTransactions;
    }

    public void setPropLongRunningTransactions(double propLongRunningTransactions) {
        Config.propLongRunningTransactions = propLongRunningTransactions;
    }

    @Override
    public String toString() {
        return "\n" +
                "    cluster size: " + clusterSize + "\n" +
                "    epoch timeout (ms): " + getEpochTimeoutInMillis() + "\n" +
                "    average commit operation rate (ms): " + getCommitOperationRateInMillis() + "\n" +
                "    average abort operation rate (ms): " + getAbortOperationRateInMillis() + "\n" +
                "    average transaction service rate (ms): " + getTransactionServiceRateInMillis() + "\n" +
                "    distributed transactions (%): " + propDistributedTransactions * 100 + "\n" +
                "    long running transactions (%): " + propLongRunningTransactions * 100 + "\n" +
                "    set seed: " + fixSeed;
    }
}
