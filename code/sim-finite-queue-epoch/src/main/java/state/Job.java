package state;

public class Job
{
    private final double arrivalTime;
    private double departureTime;
    private int retries;

    public Job( double arrivalTime )
    {
        this.arrivalTime = arrivalTime;
    }

    public void setDepartureTime( double departureTime )
    {
        this.departureTime = departureTime;
    }

    public void incRetries()
    {
        retries += 1;
    }

    public int getRetries()
    {
        return retries;
    }

    public double responseTime()
    {
        return departureTime - arrivalTime;
    }

    @Override
    public String toString()
    {
        return "{" +
               "a=" + String.format( "%.5f", arrivalTime ) +
               ", r=" + retries +
               '}';
    }
}
