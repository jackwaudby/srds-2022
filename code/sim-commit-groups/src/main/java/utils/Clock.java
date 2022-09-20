package utils;

public class Clock
{
    private static final Clock instance = new Clock();
    private double clock; // (secs)

    private Clock()
    {
        clock = 0;
    }

    public static Clock getInstance()
    {
        return instance;
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
