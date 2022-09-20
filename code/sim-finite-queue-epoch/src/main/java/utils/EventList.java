package utils;

import event.AbstractEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class EventList
{
    private static final EventList instance = new EventList();
    private final LinkedList<AbstractEvent> eventList;

    private EventList()
    {
        eventList = new LinkedList<>();
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
        return eventList.removeFirst();
    }
}
