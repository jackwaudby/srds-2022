package utils;

public class Config
{
    private static final Config instance = new Config();

    private static int clusterSize;
    private static double epochTimeout;
    private static double transactionServiceRate;
    private static long seedValue = 0;
    private static boolean fixSeed = true;

    private Config()
    {

    }

    public static Config getInstance()
    {
        return instance;
    }

    public void setSeedValue( long seedValue )
    {
        Config.seedValue = seedValue;
    }

    public void setClusterSize( int clusterSize )
    {
        Config.clusterSize = clusterSize;
    }

    public int getClusterSize()
    {
        return clusterSize;
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

    public void setTransactionServiceRate( double transactionServiceRate )
    {
        Config.transactionServiceRate = transactionServiceRate;
    }

    public void setFixSeed( boolean fixSeed )
    {
        Config.fixSeed = fixSeed;
    }

    @Override
    public String toString()
    {
        return "\n" +
               "    cluster size: " + clusterSize + "\n" +
               "    epoch timeout (ms): " + getEpochTimeoutInMillis() + "\n" +
               "    average transaction service rate (ms): " + getTransactionServiceRateInMillis() + "\n" +
               "    set seed: " + fixSeed + "\n" +
               "    seed value: " + seedValue;
    }
}
