package utils;

public class Config {
    private static final Config instance = new Config();

    private static int clusterSize;
    private static double epochTimeout;
    private static double commitOperationRate;
    private static double shortTransactionServiceRate;
    private static double longTransactionServiceRate;
    private static double propLongTransactions;

    private static final long seedValue = 0;
    private static boolean fixSeed = true;

    private Config() {

    }

    public static Config getInstance() {
        return instance;
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

    public void setEpochTimeout(double epochTimeout) {
        Config.epochTimeout = epochTimeout;
    }

    public double getShortTransactionServiceRateInMillis() {
        return shortTransactionServiceRate;
    }

    public double getShortTransactionServiceRateInSecs() {
        return shortTransactionServiceRate / 1000.0;
    }

    public void setShortTransactionServiceRate(double transactionServiceRate) {
        Config.shortTransactionServiceRate = transactionServiceRate;
    }

    public double getLongTransactionServiceRateInMillis() {
        return longTransactionServiceRate;
    }

    public double getLongTransactionServiceRateInSecs() {
        return longTransactionServiceRate / 1000.0;
    }

    public void setLongTransactionServiceRate(double transactionServiceRate) {
        Config.longTransactionServiceRate = transactionServiceRate;
    }

    public double getCommitOperationRateInMillis() {
        return commitOperationRate;
    }

    public double getCommitOperationRateInSecs() {
        return commitOperationRate / 1000.0;
    }

    public void setCommitOperationRate(double twoPhaseCommitDelay) {
        Config.commitOperationRate = twoPhaseCommitDelay;
    }

    public long getSeedValue() {
        return seedValue;
    }

    public boolean isSeedSet() {
        return fixSeed;
    }

    public void setFixSeed(boolean fixSeed) {
        Config.fixSeed = fixSeed;
    }


    public double getPropLongTransactions() {
        return propLongTransactions;
    }

    public void setPropLongTransactions(double propLongRunningTransactions) {
        Config.propLongTransactions = propLongRunningTransactions;
    }

    @Override
    public String toString() {
        return "\n" +
                "    cluster size: " + clusterSize + "\n" +
                "    epoch timeout (ms): " + getEpochTimeoutInMillis() + "\n" +
                "    average commit operation rate (ms): " + getCommitOperationRateInMillis() + "\n" +
                "    average short transaction service rate (ms): " + getShortTransactionServiceRateInMillis() + "\n" +
                "    average long transaction service rate (ms): " + getLongTransactionServiceRateInMillis() + "\n" +
                "    long transactions proportion (%): " + getPropLongTransactions() * 100 + "\n" +
                "    set seed: " + fixSeed;
    }
}
