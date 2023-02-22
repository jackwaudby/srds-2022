package utils;

import event.AbstractEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EventList
{
    private static final EventList instance = new EventList();
    private final List<AbstractEvent> eventList;

    private EventList()
    {
        eventList = new ArrayList<>();
    }

    public static EventList getInstance()
    {
        return instance;
    }

    public void addEvent( AbstractEvent event )
    {
        eventList.add( event );
    }

    public AbstractEvent getNextEvent() {
        Collections.sort(eventList);
        return eventList.remove(0);
    }
}
