import jdk.jshell.spi.ExecutionControl;

import javax.naming.ldap.Control;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
Controller class
@author Andrei Popa(ap4u19@soton.ac.uk)
 */
public class Controller
{
   /* Define constants start */
   //incoming requests
   //expected incoming requests from client
   private static final String STORE_OPERATION = "STORE";
   private static final String LOAD_OPERATION = "LOAD";
   private static final String REMOVE_OPERATION = "REMOVE"; //this will also be used to send the request to the Dstores
   private static final String LIST_OPERATION = "LIST";

   //expected incoming requests from DStores
   private static final String STORE_ACK_DSTORE = "STORE_ACK";
   private static final String REMOVE_ACK_DSTORE = "REMOVE_ACK";
   private static final String JOIN_OPERATION = "JOIN";

   //outgoing requests
   //expected outgoing requests to client
   //store operation
   private static final String STORE_TO_RESPONSE= "STORE_TO";
   private static final String STORE_COMPLETE_RESPONSE = "STORE_COMPLETE";

   //load operation
   private static final String LOAD_FROM_RESPONSE = "LOAD_FROM";

   //remove operation
   private static final String REMOVE_COMPLETE_RESPONSE = "REMOVE_COMPLETE";


   //Dstore Statuses
    private static final String DSTORE_IDLE_STATUS = "IDLE";
    private static final String DSTORE_STORE_IN_PROGRESS_STATUS = "STORE_IN_PROGRESS";
    private static final String DSTORE_REMOVE_IN_PROGRESS_STATUS = "REMOVE_IN_PROGRESS";

    //Process Statuses
    private static final String REMOVE_PROCESS = "REMOVE_PROCESS";
    private static final String STORE_PROCESS = "LIST_PROCESS";

   //list operation
    private static final String LIST_RESPONSE = "LIST";

   /* Define constants end */

    private int cPort;//the port where the controller will listen on
    private int rFactor;//the minimum number of DStores that should be in use
    private int currRFator = 0;//initially will be 0
    private int timeoutPer;//the time it should wait for a DStore to do something todo look exactly what's this
    private int timeoutForNewReb;//the time it should wait for the next rebalance operation to start
    private ServerSocket socket;//the socket where all the communications will happen(both with DStore and Client)
    private List<DSTORE_DATA> dStores = new ArrayList<DSTORE_DATA>(); //for saving && keeping track of all Dstores(this is the index from the specSheet);
    private List<ONGOING_PROCESS> ongoingProcesses = new ArrayList<ONGOING_PROCESS>();


    class DSTORE_DATA
    {
        public int dPort;
        public String status = Controller.DSTORE_IDLE_STATUS;
        public List<HashMap<String, String>> files = new ArrayList<HashMap<String, String>>();

        public DSTORE_DATA(int dPort)
        {
            this.dPort = dPort;
        }

        public void updateStatus(String status) { this.status = status; }
        
        public void addFile(String fileName, String fileSize) {
            HashMap<String, String> file =  new HashMap<String, String>();
            file.put("filename", fileName);
            file.put("filesize", fileSize);
            this.files.add(file);
        }
        
        public boolean hasFile(String fileName)
        {
            for(HashMap<String, String> currFile : this.files) {
                if(currFile.get("filename").equals(fileName)) {
                    return true;
                }
            }
            
            return false;
        }
        

    }

    class INCOMING_REQUEST
    {
        public String operation;
        public Map<String, String> arguments = new HashMap<String, String>();
        public boolean invalidOperation = false;
        public boolean invalidArguments = false;

        public INCOMING_REQUEST(String request)
        {
            this.initRequestStructure(request);
        }

        private void initRequestStructure(String request)
        {
            //split the request by spaces
            String segments[] = request.split(" ");
            //get the request type
            String request_type = segments[0];
            //decide if the the request type is correct or not
            switch (request_type) {
                case Controller.LIST_OPERATION:
                    this.operation = Controller.LIST_OPERATION; //no arguments for this request type
                    break;
                case Controller.STORE_OPERATION:
                    this.operation = Controller.STORE_OPERATION;
                    this.prepareStoreOperationArg(segments);
                    break;
                case Controller.LOAD_OPERATION:
                    this.operation = Controller.LOAD_OPERATION;
                    this.prepareLoadOperation(segments);
                    break;
                case Controller.REMOVE_OPERATION:
                    this.operation = Controller.REMOVE_OPERATION;
                    this.prepareRemoveOperation(segments);
                    break;
                case Controller.JOIN_OPERATION:
                    this.operation = Controller.JOIN_OPERATION;
                    this.prepareJoinOperation(segments);
                    break;
                case Controller.STORE_ACK_DSTORE:
                    this.operation = Controller.STORE_ACK_DSTORE;
                    this.prepareStoreACKOperation(segments);
                    break;
                case Controller.REMOVE_ACK_DSTORE:
                    this.operation = REMOVE_ACK_DSTORE;
                    this.prepareRemoveACKOperation(segments);
                default:
                    this.invalidOperation = true;
                    break;
            }
        }

        private void prepareStoreOperationArg(String[] requestSegments)
        {
            //expected request : STORE filename filesize
            if (requestSegments.length != 3) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
            this.arguments.put("filesize", requestSegments[2]);
        }

        private void prepareLoadOperation(String [] requestSegments)
        {
            //expected request : LOAD filename
            if(requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareRemoveOperation(String [] requestSegments)
        {
            //expected request : REMOVE filename
            if(requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareJoinOperation(String [] requestSegments)
        {
            //expected request : JOIN port
            if(requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("port", requestSegments[1]);
        }

        private void prepareStoreACKOperation(String [] requestSegments)
        {
            //expected request : STORE_ACK filename
            if(requestSegments.length  != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareRemoveACKOperation(String [] requestSegments)
        {
            //expected request : REMOVE_ACK filename
            if(requestSegments.length  != 2) {
                this.invalidArguments = true;
                return;
            }

            this.arguments.put("filename", requestSegments[1]);
        }
    }

    class ONGOING_PROCESS
    {
        String processType;
        PrintWriter clientOutputStream;
        List<String> dPorts; //for internal use - used for data linking.
        int numberOfSentRequests; //Number of DStores that we sent requests to
        int numberOfReceivedSuccessRequests = 0; //initially will be 0 and we will increment when we receive a success response from Dstores
        int numberOfReceivedFailureRequests = 0; //initially will be 0 and we will increment when we receive a failure response from Dstores todo currently not used at all.

        public ONGOING_PROCESS(String processType, PrintWriter clientOutputStream, List<String> dPorts, int numberOfSentRequests){
            this.processType = processType;
            this.dPorts = dPorts;
            this.numberOfSentRequests = numberOfSentRequests;
            this.clientOutputStream = clientOutputStream;
        }

    }


    /**
     * The constructor for the Controller class.
     *
     * @param cPort
     * @param rFactor
     * @param timeoutPer
     * @param timeoutForNewReb
     * @author Andrei
     */
    public Controller(int cPort, int rFactor, int timeoutPer, int timeoutForNewReb) throws Exception
    {
        this.cPort = cPort;
        this.rFactor = rFactor;
        this.timeoutPer = timeoutPer;
        this.timeoutForNewReb = timeoutForNewReb;
        try {
            this.initialiseSystem();

        } catch (Exception e) {
            this.handleError(e, false);
        }
    }

    public static void main(String[] args) throws Exception
    {
        String cPort = args[0];
        String rFactor = args[1];
        String timeoutPer = args[2];
        String timeoutForNewReb = args[3];
        System.out.println("Starting server...");
        Controller sysStart = new Controller(Integer.parseInt(cPort), Integer.parseInt(rFactor), Integer.parseInt(timeoutPer), Integer.parseInt(timeoutForNewReb));
    }



    /**
     * The entrypoint of the program when the Controller starts
     *
     * @return void
     * @throws Exception
     */
    private void initialiseSystem() throws Exception
    {
       //setup the port to listen for incoming requests from client/Dstores
       this.socket = new ServerSocket(this.cPort);
        System.out.println("Controller Server started. Listening to requests.");
        //wait for incoming connections
       this.waitForRequests();
    }

    private void waitForRequests() throws Exception
    {
       for(;;) {
          Socket client = this.socket.accept();
          BufferedReader inStream = new BufferedReader(new InputStreamReader(client.getInputStream()));
          PrintWriter outStream = new PrintWriter(client.getOutputStream());

          String request;
          while((request = inStream.readLine()) != null) {
             System.out.println("Incoming request : " + request);
             INCOMING_REQUEST formattedRequest = new INCOMING_REQUEST(request);

             if(formattedRequest.invalidOperation || formattedRequest.invalidArguments)
                throw new Exception("Invalid request: " + request); //todo change here with the message that is expected

              String response;
              switch (formattedRequest.operation) {
                  case Controller.LIST_OPERATION:
                      response = this.processListOperation();
                      this.sendResponse(response, outStream);
                      break;
                  case Controller.STORE_OPERATION:
                      response = this.processStoreOperation(formattedRequest.arguments.get("filename"), formattedRequest.arguments.get("filesize"), outStream);
                      this.sendResponse(response, outStream);
                      break;
                  case Controller.LOAD_OPERATION:
                      response = this.processLoadOperation(formattedRequest.arguments.get("filename"));
                      this.sendResponse(response, outStream);
                      break;
                  case Controller.REMOVE_OPERATION:
                      //no response here because this will be covered in the REMOVE_ACK CASE
                      this.processRemoveOperation(formattedRequest.arguments.get("filename"), outStream);
                      break;
                  case Controller.JOIN_OPERATION:
                      this.processJoinOperation(formattedRequest.arguments.get("port"));
                      break;
                  case Controller.STORE_ACK_DSTORE:
                      this.processStoreACKOperation(formattedRequest.arguments.get("filename"), client.getPort());
                      break;
                  case Controller.REMOVE_ACK_DSTORE:
                      this.processRemoveACKOperation(formattedRequest.arguments.get("filename"), client.getPort());
              }

          }
          client.close();
       }


    }

    //LIST OPERATION
    private String processListOperation() throws Exception
    {
        this.checkIfEnoughDstores(false);
        String rtrn_request = Controller.LIST_RESPONSE + " ";
        ArrayList<String> usedFiles = new ArrayList<String>();
        List<HashMap<String, String>> currFileList;
        String currFileName;

        //because a dstore might not be up to date with the other dstores, we need to see what files are in the overall distributed sever storage
        for(DSTORE_DATA eachDstore : this.dStores) {
            currFileList = eachDstore.files;
            for (HashMap<String, String> eachFile : currFileList) {
                currFileName = eachFile.get("filename");
                if(!usedFiles.contains(currFileName)) {
                    usedFiles.add(currFileName);
                }
            }
        }

        int i = 1;
        String currAppend;

        for (String filename : usedFiles) {
            currAppend = filename + ((i == usedFiles.size()) ? " " : "");

            rtrn_request += currAppend;
            i++;
        }


        return  rtrn_request;
    }


    //STORE OPERATION
    private String processStoreOperation(String filename, String filesize, PrintWriter clientOutputStream) throws Exception
    {
        this.checkIfEnoughDstores(true);
        String port_arr = " ";
        String rtrn_request = Controller.STORE_TO_RESPONSE;

        List<DSTORE_DATA> selectedDstores = new ArrayList<DSTORE_DATA>();

        //select R dStores && update their status
        for(int i = 0; i < this.dStores.size(); i++){
            this.dStores.get(i).updateStatus(Controller.DSTORE_STORE_IN_PROGRESS_STATUS);
            this.dStores.get(i).addFile(filename, filesize);
            selectedDstores.add(this.dStores.get(i));
        }
        //get their port and put in the request
        String currAppend;
        List<String> selectedDPorts = new ArrayList<String>();
        int i = 1;
        for (DSTORE_DATA eachDstore : selectedDstores) {
            currAppend = Integer.toString(eachDstore.dPort) + ((i == selectedDstores.size()) ? "" : " ");
            selectedDPorts.add(Integer.toString(eachDstore.dPort));
            port_arr += currAppend;
        }

        //create a new process so that we can keep track of the ongoing ones
        ONGOING_PROCESS storeProcess = new ONGOING_PROCESS(Controller.STORE_PROCESS, clientOutputStream, selectedDPorts, selectedDPorts.size());
        this.ongoingProcesses.add(storeProcess);

        //port arr would look something like " por1 port2 port3 ..."
        rtrn_request += port_arr;

        //request should look something like "STORE_TO port1 port2 … portR"
        return rtrn_request;
    }


    //LOAD OPERATION
    private String processLoadOperation(String filename) throws Exception
    {
        this.checkIfEnoughDstores(false);
        String rtrn_request = Controller.LOAD_FROM_RESPONSE;
        //get the first dStore that has the file

        DSTORE_DATA selectedDstore = null;
        
        for(DSTORE_DATA eachDstore : this.dStores) {
            if(eachDstore.hasFile(filename)){
                selectedDstore = eachDstore;
                break;
            }
        }
        
        if (selectedDstore == null) {
            throw  new Exception("the file is not on the server");
        }
        

        String append = " ";

        append += Integer.toString(selectedDstore.dPort);
        rtrn_request += append;
        //request should look something like "LOAD_FROM port filesize"
        return rtrn_request;
    }

    //REMOVE OPERATION
    private void processRemoveOperation(String filename, PrintWriter clientOutputStream) throws Exception
    {
        this.checkIfEnoughDstores(false);

        ArrayList<String> dStorePorts  = new ArrayList<String>();
        //select all dStores && update their status
        for(int i = 0; i < this.dStores.size(); i++){
            this.dStores.get(i).updateStatus(Controller.DSTORE_REMOVE_IN_PROGRESS_STATUS);
            dStorePorts.add(Integer.toString(this.dStores.get(i).dPort));
        }

        String dStorePayload = Controller.REMOVE_OPERATION + " " + filename;


        //create a new process so that we can keep track of the ongoing ones
        ONGOING_PROCESS removeProcess = new ONGOING_PROCESS(Controller.REMOVE_PROCESS, clientOutputStream, dStorePorts, dStorePorts.size());
        this.ongoingProcesses.add(removeProcess);

        //now for each the selected dStores send remove requests to them.
        //NOTE that we will just send the payload, the response will come afterwards.
        for (String currDPort: dStorePorts) {
            //create a new connection with the DStore
            Socket currSocket = new Socket("localhost",Integer.parseInt(currDPort));
            PrintWriter currOutPutStream = new PrintWriter(currSocket.getOutputStream());
            currOutPutStream.println(dStorePayload);
            currOutPutStream.flush();
        }

    }

    //JOIN OPERATION
    private void processJoinOperation(String port)
    {
        DSTORE_DATA dStore = new DSTORE_DATA(Integer.parseInt(port));
        this.dStores.add(dStore);
        this.currRFator++;
    }

    //STORE ACK OPERATION
    private void processStoreACKOperation(String filename, int dPort) throws Exception
    {
        ONGOING_PROCESS linkedProcess = this.getOngoingProcessByDPort(dPort, Controller.STORE_PROCESS);
        linkedProcess.numberOfReceivedSuccessRequests++;

        //if we have received all success responses from Dstores send the success response to client
        if(linkedProcess.numberOfReceivedSuccessRequests == linkedProcess.numberOfSentRequests) {
            String response = Controller.STORE_COMPLETE_RESPONSE;
            linkedProcess.clientOutputStream.println(response);
            linkedProcess.clientOutputStream.flush();
            this.removeOngoingProcess(linkedProcess);
        }

    }
    //REMOVE ACK OPERATION
    private void processRemoveACKOperation(String filename, int dPort) throws Exception
    {
        ONGOING_PROCESS linkedProcess = this.getOngoingProcessByDPort(dPort, Controller.REMOVE_PROCESS);
        linkedProcess.numberOfReceivedSuccessRequests++;

        //if we have received all success responses from Dstores send the success response to client
        if(linkedProcess.numberOfReceivedSuccessRequests == linkedProcess.numberOfSentRequests) {
            String response = Controller.REMOVE_COMPLETE_RESPONSE;
            linkedProcess.clientOutputStream.println(response);
            linkedProcess.clientOutputStream.flush();
            this.removeOngoingProcess(linkedProcess);
        }

    }

    private ONGOING_PROCESS getOngoingProcessByDPort(int dPort, String process_type) throws Exception
    {
        for (ONGOING_PROCESS currOngoingProcess : this.ongoingProcesses) {
            if(currOngoingProcess.processType.equals(process_type)) {
                if(currOngoingProcess.dPorts.contains(Integer.toString(dPort))) {
                    return currOngoingProcess;
                }
            }
        }
        throw new Exception("unknown dPort");
    }

    private void removeOngoingProcess(ONGOING_PROCESS processToRemove)
    {
        int  i = 0;
        for (ONGOING_PROCESS currOngoingProcess : this.ongoingProcesses) {
            if(currOngoingProcess == processToRemove) {
                this.ongoingProcesses.remove(i);
                break;
            }
            i++;
        }
    }


    private void checkIfEnoughDstores(boolean idleStatus) throws Exception
    {
        int dStores = idleStatus ? this.currRFator : 0;

        //for store operation we need at least R dStores in the "idle status"
        if(idleStatus) {
            for (DSTORE_DATA eachDstore : this.dStores) {
                if(eachDstore.status.equals(Controller.DSTORE_IDLE_STATUS)) {
                    dStores++;
                }
            }
        }

        boolean result = this.currRFator >= this.rFactor;

        if(!result) {
            throw new Exception("not enough DStores");
        }
    }



    private String getRequestType(String request)
    {
       return null;
    }


    /**
     * The general function that will handle the error types && log the respective data associated with that error.
     *
     * @param e
     * @param throwError
     * @return void
     * @throws Error
     * @author Andrei
     */
    private void handleError(Exception e, boolean throwError) throws Exception
    {

    }

    private void sendResponse(String response, PrintWriter outStream)
    {
        outStream.println(response);
        outStream.flush();
    }


//todo implement rebalance operations when everything else if finished

}
