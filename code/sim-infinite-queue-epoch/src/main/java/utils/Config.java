package utils;

public class Config
{
    private static final Config instance = new Config();

    private static int clusterSize;
    private static double epochTimeout;
    private static double commitOperationRate;
    private static double abortOperationRate;
    private static double transactionServiceRate;
    private static double failureRate;
    private static double repairRate;
    private static double propDistributedTransactions;
    private static final long seedValue = 0;
    private static boolean fixSeed = true;
    private static boolean affinity = false;
    private static String algorithm = "single";
    private static boolean fixedEpochTimeout = true;

    private Config()
    {

    }

    public static Config getInstance()
    {
        return instance;
    }

    public boolean isFixedEpochTimeout()
    {
        return fixedEpochTimeout;
    }

    public void setFixedEpochTimeout( boolean fixedEpochTimeout )
    {
        Config.fixedEpochTimeout = fixedEpochTimeout;
    }

    public boolean isAffinity()
    {
        return affinity;
    }

    public void setAffinity( boolean affinity )
    {
        Config.affinity = affinity;
    }

    public void setAlgorithm( String algorithm )
    {
        Config.algorithm = algorithm;
    }

    public String getAlgorithm()
    {
        return algorithm;
    }

    public void setClusterSize( int clusterSize )
    {
        Config.clusterSize = clusterSize;
    }

    public int getClusterSize()
    {
        return clusterSize;
    }

    public double getRepairRateInMillis()
    {
        return repairRate;
    }

    public double getRepairRateInSecs()
    {
        return repairRate / 1000.0;
    }

    public double getFailureRateInMillis()
    {
        return failureRate;
    }

    public double getFailureRateInSecs()
    {
        return failureRate / 1000.0;
    }

    public double getEpochTimeoutInMillis()
    {
        return epochTimeout;
    }

    public double getEpochTimeoutInSecs()
    {
        return epochTimeout / 1000.0;
    }

    public double getTransactionServiceRateInMillis()
    {
        return transactionServiceRate;
    }

    public double getTransactionServiceRateInSecs()
    {
        return transactionServiceRate / 1000.0;
    }

    public double getCommitOperationRateInMillis()
    {
        return commitOperationRate;
    }

    public double getCommitOperationRateInSecs()
    {
        return commitOperationRate / 1000.0;
    }

    public double getAbortOperationRateInMillis()
    {
        return abortOperationRate;
    }

    public double getAbortOperationRateInSecs()
    {
        return abortOperationRate / 1000.0;
    }

    public long getSeedValue()
    {
        return seedValue;
    }

    public boolean isSeedSet()
    {
        return fixSeed;
    }

    public void setEpochTimeout( double epochTimeout )
    {
        Config.epochTimeout = epochTimeout;
    }

    public void setCommitOperationRate( double twoPhaseCommitDelay )
    {
        Config.commitOperationRate = twoPhaseCommitDelay;
    }

    public void setAbortOperationRate( double abortOperationRate )
    {
        Config.abortOperationRate = abortOperationRate;
    }

    public void setTransactionServiceRate( double transactionServiceRate )
    {
        Config.transactionServiceRate = transactionServiceRate;
    }

    public void setFailureRate( double failureRate )
    {
        Config.failureRate = failureRate;
    }

    public void setRepairRate( double repairRate )
    {
        Config.repairRate = repairRate;
    }

    public void setFixSeed( boolean fixSeed )
    {
        Config.fixSeed = fixSeed;
    }

    public double getPropDistributedTransactions()
    {
        return propDistributedTransactions;
    }

    public void setPropDistributedTransactions( double propDistributedTransactions )
    {
        Config.propDistributedTransactions = propDistributedTransactions;
    }

    @Override
    public String toString()
    {
        return "\n" +
               "    cluster size: " + clusterSize + "\n" +
               "    epoch timeout (ms): " + getEpochTimeoutInMillis() + "\n" +
               "    average commit operation rate (ms): " + getCommitOperationRateInMillis() + "\n" +
               "    average abort operation rate (ms): " + getAbortOperationRateInMillis() + "\n" +
               "    average transaction service rate (ms): " + getTransactionServiceRateInMillis() + "\n" +
               "    average failure rate (ms): " + getFailureRateInMillis() + "\n" +
               "    average repair rate (ms): " + getRepairRateInMillis() + "\n" +
               "    distributed transactions (%): " + propDistributedTransactions * 100 + "\n" +
               "    set seed: " + fixSeed + "\n" +
               "    affinity: " + affinity + "\n" +
               "    algorithm: " + algorithm;
    }
}
