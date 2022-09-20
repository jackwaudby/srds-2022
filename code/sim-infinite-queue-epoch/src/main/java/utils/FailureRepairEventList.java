package utils;

import event.AbstractEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FailureRepairEventList
{
    private static final FailureRepairEventList instance = new FailureRepairEventList();
    private final List<AbstractEvent> eventList;

    private FailureRepairEventList()
    {
        eventList = new ArrayList<>();
    }

    public static FailureRepairEventList getInstance()
    {
        return instance;
    }

    public void addEvent( AbstractEvent event )
    {
        eventList.add( event );
    }

    public double getNextEventTime()
    {
        Collections.sort( eventList );
        return eventList.get( 0 ).getEventTime();
    }

    public void removeEvent( AbstractEvent event )
    {
        eventList.remove( event );
    }

    public boolean isFailureOrRepairBefore(double end) {
        for ( var event: eventList
               )
        {
            if (event.getEventTime() < end) {
                return true;
            }
        }
        return false;
    }
}
