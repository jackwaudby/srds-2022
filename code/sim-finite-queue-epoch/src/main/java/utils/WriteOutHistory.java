package utils;

import state.Cluster;
import state.CommitGroup;
import state.EpochState;
import state.TerminalState;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.stream.Collectors;

public class WriteOutHistory
{

    public static void writeOutHistory()
    {

        BufferedWriter outputStream = null;
        FileWriter fileWriter;
        try
        {
            File file = new File( "debug.log" );

            if ( !file.exists() )
            {
                fileWriter = new FileWriter( file, true );
                outputStream = new BufferedWriter( fileWriter );
                for ( var epoch : Cluster.getInstance().getHistory()
                )
                {
                    if ( epoch.getEpochState() == TerminalState.PARTIAL_FAILURE || epoch.getEpochState() == TerminalState.TOTAL_FAILURE )
                    {

                        var start = epoch.getStart();
                        var duration = epoch.getFailures().stream().map( f -> f.getEventTime() - start ).collect( Collectors.toList() ).get( 0 );

                        var res = String.format( "{id=%s, len=%.5f, committed=%s, aborted=%s, state=%s",
                                epoch.getEpochId(), duration,
                                epoch.getCompletedTransactions(), epoch.getAbortedTransactions(),
                                epoch.getEpochState() );

                        outputStream.append( res );
                        outputStream.append( "\n" );
                    }
                }
            }
            else
            {
                fileWriter = new FileWriter( file, true );
                outputStream = new BufferedWriter( fileWriter );
                try
                {
                    for ( var epoch : Cluster.getInstance().getHistory()
                    )
                    {
                        if ( !epoch.getFailures().isEmpty() && epoch.getEpochState() == TerminalState.PARTIAL_FAILURE )
                        {
                            outputStream.append( epoch.toString() );
                            outputStream.append( "\n" );
                        }
                    }
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        finally
        {
            if ( outputStream != null )
            {
                try
                {
                    outputStream.flush();
                    outputStream.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }
    }
}
