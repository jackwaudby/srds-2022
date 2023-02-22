package utils;

public class Clock
{
    private static final Clock instance = new Clock();
    private double clock; // (secs)
    private double simStartTime;

    private Clock()
    {
        clock = 0;
        simStartTime = 0;
    }

    public static Clock getInstance()
    {
        return instance;
    }

    public void setSimStartTime( double simStartTime )
    {
        this.simStartTime = simStartTime;
    }

    public double getSimStartTime()
    {
        return simStartTime;
    }

    public void setClock( double eventTime )
    {
        clock = eventTime;
    }

    public double getClock()
    {
        return clock;
    }
}
