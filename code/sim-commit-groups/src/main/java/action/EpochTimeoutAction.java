package action;

import org.apache.log4j.Logger;
import state.Cluster;
import state.EpochState;

public class EpochTimeoutAction
{
    private final static Logger LOGGER = Logger.getLogger( EpochTimeoutAction.class.getName() );

    public static void timeout( Cluster cluster )
    {
        cluster.setCurrentEpochState( EpochState.WAITING );
    }
}
