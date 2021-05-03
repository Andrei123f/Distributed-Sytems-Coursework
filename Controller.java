/*
The Controller class.
This class will handle : todo add here

The Controller is started first, with R as an argument. It waits for Dstores to join the
datastore (see Rebalance operation). The Controller does not serve any client request
until at least R Dstores have joined the system.
As Dstores may fail and be restarted, and new Dstores can join the datastore at runtime,
rebalance operations are required to make sure each file is replicated R times and files
are distributed evenly over the Dstores.


The Dstores will establish connections with the Controller as soon as they start. These
connections will be persistent. If the Controller detects that the connection with one of the
Dstores dropped, then such a Dstore will be removed from the set of Dstores that joined
the data store


Processes should send textual messages (e.g., LIST) using the println() method of
PrintWriter class, and receive using the readLine() method of BufferedReader class. For
data messages (i.e., file content), processes should send using the write() method of
OutputStream class and receive using the readNBytes() method of InputStream class.


@author Andrei Popa(ap4u19@soton.ac.uk)
 */
public class Controller
{

   /*
   ControllerLogger for logging the messages



    Constructor
    java Controller cport R timeout rebalance_period


    Where the Controller is given a port to listen on (cport), a replication factor (R), a
    timeout in milliseconds (timeout) and how long to wait (in milliseconds) to start the next
    rebalance operation (rebalance_period)



    */
public Controller()
{





//todo implement rebalance operations when everything else if finished
}
}
