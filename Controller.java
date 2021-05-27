import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Controller class - this will be the entrypoint of the Distributed Storage.
 * A singleton design pattern will be used for this class, where each property will be accessible && dynamically changed by the request threads.
 *
 * @author Andrei23f(ap4u19@soton.ac.uk)
 */
public class Controller {
    /* Define constants start */
    //incoming requests
    //expected incoming requests from client
    private static final String STORE_OPERATION = Protocol.STORE_TOKEN;
    private static final String LOAD_OPERATION = Protocol.LOAD_TOKEN;
    private static final String REMOVE_OPERATION = Protocol.REMOVE_TOKEN; //this will also be used to send the request to the Dstores
    private static final String LIST_OPERATION = Protocol.LIST_TOKEN;

    //expected incoming requests from DStores
    private static final String STORE_ACK_DSTORE = Protocol.STORE_ACK_TOKEN;
    private static final String REMOVE_ACK_DSTORE = Protocol.REMOVE_ACK_TOKEN;
    public static final String JOIN_OPERATION = Protocol.JOIN_TOKEN;

    //outgoing requests
    //expected outgoing requests to client
    //store operation
    private static final String STORE_TO_RESPONSE = "STORE_TO";
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

    //error types for Controller
    private static final String ERROR_FILE_ALREADY_EXISTS = "ERROR_FILE_ALREADY_EXISTS";
    private static final String ERROR_FILE_DOES_NOT_EXIST = "ERROR_FILE_DOES_NOT_EXIST";
    private static final String ERROR_NOT_ENOUGH_DSTORES = "ERROR_NOT_ENOUGH_DSTORES";

    /* Define constants end */

    static private int cPort;//the port where the controller will listen on
    static private int rFactor;//the minimum number of DStores that should be in use
    static private int currRFator = 0;//initially will be 0
    static private int timeoutPer;//the time it should wait for a DStore to do something todo look exactly what's this
    static private int timeoutForNewReb;//the time it should wait for the next rebalance operation to start
    static private ServerSocket socket;//the socket where all the communications will happen(both with DStore and Client)
    static private List<DSTORE_DATA> dStores = new ArrayList<DSTORE_DATA>(); //for saving && keeping track of all Dstores(this is the index from the specSheet);
    static private List<ONGOING_PROCESS> ongoingProcesses = new ArrayList<ONGOING_PROCESS>();
    static private Object lock = new Object();//for thread locking

    //

    /**
     * The class that represents each Database. This will be dynamically updated based on Client's request && Dstore's responses.
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    class DSTORE_DATA {
        public int dPort;
        public String status = Controller.DSTORE_IDLE_STATUS;
        public List<HashMap<String, String>> files = new ArrayList<HashMap<String, String>>();

        public DSTORE_DATA(int dPort) {
            this.dPort = dPort;
        }

        public void updateStatus(String status) {
            this.status = status;
        }

        public void addFile(String fileName, String fileSize) {
            HashMap<String, String> file = new HashMap<String, String>();
            file.put("filename", fileName);
            file.put("filesize", fileSize);
            this.files.add(file);
        }

        public void removeFile(String filename) {
            int i = 0;
            for (HashMap<String, String> currFile : this.files) {
                if(currFile.get("filename").equals(filename)) {
                    this.files.remove(i);
                    break;
                }
                i++;
            }
        }

        public boolean hasFile(String fileName) {
            for (HashMap<String, String> currFile : this.files) {
                if (currFile.get("filename").equals(fileName)) {
                    return true;
                }
            }

            return false;
        }

        public HashMap<String, String> getFileInfoByFileName(String filename)
        {
            int i = 0;
            for (HashMap<String, String> currFile : this.files) {
                if(currFile.get("filename").equals(filename)) {
                    return this.files.get(i);
                }
                i++;
            }
            return null;
        }

    }

    /**
     * The class that will validate && format each request that comes from Client / Dstores.
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    class INCOMING_REQUEST {
        public String operation;
        public Map<String, String> arguments = new HashMap<String, String>();
        public boolean invalidOperation = false;
        public boolean invalidArguments = false;

        public INCOMING_REQUEST(String request) {
            this.initRequestStructure(request);
        }

        private void initRequestStructure(String request) {
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
                    this.operation = Controller.REMOVE_ACK_DSTORE;
                    this.prepareRemoveACKOperation(segments);
                    break;
                default:
                    this.invalidOperation = true;
                    break;
            }
        }

        private void prepareStoreOperationArg(String[] requestSegments) {
            //expected request : STORE filename filesize
            if (requestSegments.length != 3) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
            this.arguments.put("filesize", requestSegments[2]);
        }

        private void prepareLoadOperation(String[] requestSegments) {
            //expected request : LOAD filename
            if (requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareRemoveOperation(String[] requestSegments) {
            //expected request : REMOVE filename
            if (requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareJoinOperation(String[] requestSegments) {
            //expected request : JOIN port
            if (requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("port", requestSegments[1]);
        }

        private void prepareStoreACKOperation(String[] requestSegments) {
            //expected request : STORE_ACK filename
            if (requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareRemoveACKOperation(String[] requestSegments) {
            //expected request : REMOVE_ACK filename
            if (requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }
    }

    /**
     * The class that will be used for keeping track of ongoing Thread requests from the Client(Store and Remove).
     * This will be updated/deleted once we get every ACK response from the Dstores.
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    class ONGOING_PROCESS {
        String processType;
        PrintWriter clientOutputStream;
        List<String> dPorts; //for internal use - used for data linking.
        int numberOfSentRequests; //Number of DStores that we sent requests to
        //volatile int numberOfReceivedSuccessRequests = 0; //initially will be 0 and we will increment when we receive a success response from Dstores
        private final AtomicInteger numberOfReceivedSuccessRequests = new AtomicInteger(0);
        int numberOfReceivedFailureRequests = 0; //initially will be 0 and we will increment when we receive a failure response from Dstores todo currently not used at all.
        public long ThreadId;
        public String fileName;

        public ONGOING_PROCESS(String processType, PrintWriter clientOutputStream, List<String> dPorts, int numberOfSentRequests, long ThreadId) {
            this.processType = processType;
            this.dPorts = dPorts;
            this.numberOfSentRequests = numberOfSentRequests;
            this.clientOutputStream = clientOutputStream;
            this.ThreadId = ThreadId;
        }

    }

    /**
     * The class that will be used for each request because each Request will have its own thread and common used variables from the Controller object.
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    class REQUEST_THREAD implements Runnable {
        private Socket clientSocket;
        private BufferedReader inStream;
        private PrintWriter outStream;
        private boolean finishedOngoingRequest = false;
        private ONGOING_PROCESS currentOngoingProcess;
        private String request_type;//for handling when a dstore connection drops.
        private String dPort; //for handling when a dstore connection drops
        private List<DSTORE_DATA> dstoresAffected = new ArrayList<DSTORE_DATA>();

        public REQUEST_THREAD(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        /**
         * Entry point for each request thread. We need to : 1) get the request, 2) validate && format it, 3) process it
         * @author Andrei123f(ap4u19@soton.ac.uk)
         */
        public void run() {
            try {
                this.inStream = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
                this.outStream = new PrintWriter(this.clientSocket.getOutputStream());
                String operation = null;
                String request = this.inStream.readLine();

                while (request  != null) {
                    try {
                        System.out.println("Incoming request : " + request);
                        ControllerLogger.getInstance().log("Incoming request : " + request);
                        INCOMING_REQUEST formattedRequest = new INCOMING_REQUEST(request);

                        if (formattedRequest.invalidOperation || formattedRequest.invalidArguments) {
                            throw new Exception("Invalid request: " + request);
                        }


                        String response;
                        operation = formattedRequest.operation;
                        this.request_type = operation;
                        switch (formattedRequest.operation) {
                            case Controller.LIST_OPERATION:
                                Thread.currentThread().setPriority(7);
                                response = this.processListOperation();
                                this.sendResponse(response, outStream);
                                break;
                            case Controller.STORE_OPERATION:
                                response = this.processStoreOperation(formattedRequest.arguments.get("filename"), formattedRequest.arguments.get("filesize"), outStream);
                                this.sendResponse(response, outStream);
                                while (!this.finishedOngoingRequest) {
                                    //check if we have finished.
                                    this.checkIfSuccessStoreComplete();
                                    //update the ongoing process
                                    this.updateOngoingRequest(Controller.STORE_PROCESS);
                                    Thread.sleep(10);
                                }
                                this.sendResponse(Controller.STORE_COMPLETE_RESPONSE, outStream);
                                synchronized (Controller.lock) {
                                    Controller.addFileToDstores(this.dstoresAffected, formattedRequest.arguments.get("filename"), formattedRequest.arguments.get("filesize"));
                                    Controller.updateDstoreStatuses(this.dstoresAffected, Controller.DSTORE_IDLE_STATUS);
                                    Controller.removeOngoingProcess(this.currentOngoingProcess);
                                    this.dstoresAffected = new ArrayList<DSTORE_DATA>();
                                }
                                break;
                            case Controller.LOAD_OPERATION:
                                response = this.processLoadOperation(formattedRequest.arguments.get("filename"));
                                this.sendResponse(response, outStream);
                                break;
                            case Controller.REMOVE_OPERATION:
                                this.processRemoveOperation(formattedRequest.arguments.get("filename"), outStream);
                                while (!this.finishedOngoingRequest) {
                                    //check if we have finished.
                                    this.checkIfSuccessRemoveComplete();
                                    //update the ongoing process
                                    this.updateOngoingRequest(Controller.REMOVE_PROCESS);
                                    Thread.sleep(10);
                                }
                                this.sendResponse(Controller.REMOVE_COMPLETE_RESPONSE, outStream);
                                synchronized (Controller.lock) {
                                    Controller.removeFileFromDstores(this.dstoresAffected, formattedRequest.arguments.get("filename"));
                                    Controller.updateDstoreStatuses(this.dstoresAffected, Controller.DSTORE_IDLE_STATUS);
                                    Controller.removeOngoingProcess(this.currentOngoingProcess);
                                    this.dstoresAffected = new ArrayList<DSTORE_DATA>();
                                }
                                break;
                            case Controller.JOIN_OPERATION:
                                Thread.currentThread().setPriority(7);
                                this.processJoinOperation(formattedRequest.arguments.get("port"));
                                break;
                            case Controller.STORE_ACK_DSTORE:
                                Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                                this.processStoreACKOperation(formattedRequest.arguments.get("filename"), this.clientSocket.getPort());
                                break;
                            case Controller.REMOVE_ACK_DSTORE:
                                Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                                this.processRemoveACKOperation(formattedRequest.arguments.get("filename"), clientSocket.getPort());
                                break;
                        }
                    } catch (Throwable e) {
                        this.processError((e.getMessage() != null ? e.getMessage() : e.toString()), outStream);
                    }

                    request = this.inStream.readLine();
                }
                System.out.println("Session closed for operation : " + this.request_type + " curr N : " + Controller.currRFator);

                //for the cases when the session is either terminated/timed out/finished by the client/dstore
                switch (this.request_type) {
                    case Controller.JOIN_OPERATION:
                        System.out.println("A connection with a Dstore(port : " + this.dPort  + ") just dropped. Updating index ... ");
                        this.processDstoreDrop();
                        System.out.println("Index updated.");
                        break;
                    case Controller.STORE_OPERATION:
                        if(!this.finishedOngoingRequest) {
                            System.out.println("Store operation has timed out/has been closed without finishing. Updating index.");
                        }
                    case Controller.REMOVE_OPERATION:
                        if(!this.finishedOngoingRequest) {
                            System.out.println("Remove operation has timed out/has been closed without finishing. Updating index.");
                        }
                    default:
                        synchronized (Controller.lock) {
                            Controller.checkIfDstoreConnectionExists();
                        }
                }




            } catch (Throwable e) {
                this.processError((e.getMessage() != null ? e.getMessage() : e.toString()), outStream);
            }
        }


        //JOIN OPERATION
        private void processJoinOperation(String port) {
            synchronized (Controller.lock) {
                System.out.println("adding a new DStore...");
                DSTORE_DATA dStore = new DSTORE_DATA(Integer.parseInt(port));
                Controller.dStores.add(dStore);
                Controller.currRFator++;
                System.out.println("DStore added.");
                this.dPort = port;
                //Controller.printDstoreData();
            }

        }

        //LIST OPERATION
        private String processListOperation() throws Exception {
            synchronized (Controller.lock)
            {
                Controller.checkIfEnoughDstores(false);
                Controller.printDstoreData();
                String rtrn_request = Controller.LIST_RESPONSE + " ";
                ArrayList<String> usedFiles = new ArrayList<String>();
                List<HashMap<String, String>> currFileList;
                String currFileName;

                //because a dstore might not be up to date with the other dstores, we need to see what files are in the overall distributed sever storage
                for (DSTORE_DATA eachDstore : Controller.dStores) {
                    currFileList = eachDstore.files;
                    for (HashMap<String, String> eachFile : currFileList) {
                        currFileName = eachFile.get("filename");
                        if (!usedFiles.contains(currFileName)) {
                            usedFiles.add(currFileName);
                        }
                    }
                }

                int i = 1;
                String currAppend;

                for (String filename : usedFiles) {
                    currAppend = filename + ((i == usedFiles.size()) ? "" : " ");

                    rtrn_request += currAppend;
                    i++;
                }
                return rtrn_request;
            }
        }

        //STORE OPERATION
        private String processStoreOperation(String filename, String filesize, PrintWriter clientOutputStream) throws Exception {
            synchronized (Controller.lock) {
                Controller.checkIfEnoughDstores(true);
                if (Controller.checkIfFileExists(filename)) {
                    throw new Exception(Controller.ERROR_FILE_ALREADY_EXISTS);
                }
                String port_arr = " ";
                String rtrn_request = Controller.STORE_TO_RESPONSE;

                List<DSTORE_DATA> selectedDstores = new ArrayList<DSTORE_DATA>();

                //select R dStores && update their status
                for (int i = 0; i < Controller.dStores.size(); i++) {
                    Controller.dStores.get(i).updateStatus(Controller.DSTORE_STORE_IN_PROGRESS_STATUS);
                    //Controller.dStores.get(i).addFile(filename, filesize); we will add the file once we receive all ACK responses.
                    selectedDstores.add(Controller.dStores.get(i));
                    this.dstoresAffected.add(Controller.dStores.get(i));
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
                ONGOING_PROCESS storeProcess = new ONGOING_PROCESS(Controller.STORE_PROCESS, clientOutputStream, selectedDPorts, selectedDPorts.size(), Thread.currentThread().getId());
                storeProcess.fileName = filename;
                Controller.ongoingProcesses.add(storeProcess);
                this.currentOngoingProcess = storeProcess;

                //port arr would look something like " por1 port2 port3 ..."
                rtrn_request += port_arr;

                //request should look something like "STORE_TO port1 port2 … portR"
                return rtrn_request;
            }
        }

        //REMOVE OPERATION
        private void processRemoveOperation(String filename, PrintWriter clientOutputStream) throws Exception {
            synchronized (Controller.lock) {
                Controller.checkIfEnoughDstores(false);
                if (!Controller.checkIfFileExists(filename)) {
                    throw new Exception(Controller.ERROR_FILE_DOES_NOT_EXIST);
                }

                ArrayList<String> dStorePorts = new ArrayList<String>();
                //select all dStores && update their status
                for (int i = 0; i < Controller.dStores.size(); i++) {
                    Controller.dStores.get(i).updateStatus(Controller.DSTORE_REMOVE_IN_PROGRESS_STATUS);
                    dStorePorts.add(Integer.toString(Controller.dStores.get(i).dPort));
                    this.dstoresAffected.add(Controller.dStores.get(i));
                }

                String dStorePayload = Controller.REMOVE_OPERATION + " " + filename;


                //create a new process so that we can keep track of the ongoing ones
                ONGOING_PROCESS removeProcess = new ONGOING_PROCESS(Controller.REMOVE_PROCESS, clientOutputStream, dStorePorts, dStorePorts.size(), Thread.currentThread().getId());
                removeProcess.fileName = filename;
                Controller.ongoingProcesses.add(removeProcess);
                this.currentOngoingProcess = removeProcess;

                //now for each the selected dStores send remove requests to them.
                //NOTE that we will just send the payload, the response will come afterwards.
                for (String currDPort : dStorePorts) {
                    //create a new connection with the DStore
                    Socket currSocket = new Socket("localhost", Integer.parseInt(currDPort));
                    PrintWriter currOutPutStream = new PrintWriter(currSocket.getOutputStream());
                    currOutPutStream.println(dStorePayload);
                    currOutPutStream.flush();
                }
            }

        }

        //LOAD OPERATION
        private String processLoadOperation(String filename) throws Exception {
            synchronized (Controller.lock)
            {
                Controller.checkIfEnoughDstores(false);
                String rtrn_request = Controller.LOAD_FROM_RESPONSE;
                //get the first dStore that has the file

                DSTORE_DATA selectedDstore = null;
                String filesize = "";

                for (DSTORE_DATA eachDstore : Controller.dStores) {
                    if (eachDstore.hasFile(filename)) {
                        selectedDstore = eachDstore;
                        filesize = eachDstore.getFileInfoByFileName(filename).get("filesize");
                        break;
                    }
                }

                if (selectedDstore == null) {
                    throw new Exception(Controller.ERROR_FILE_DOES_NOT_EXIST);
                }


                String append = " ";

                append += Integer.toString(selectedDstore.dPort);
                rtrn_request += append;
                rtrn_request += " ";
                rtrn_request += filesize;
                //request should look something like "LOAD_FROM port filesize"
                return rtrn_request;
            }
        }

        //STORE ACK OPERATION
        private void processStoreACKOperation(String filename, int dPort) throws Exception {
            synchronized (Controller.lock)
            {
                ONGOING_PROCESS linkedProcess = Controller.getOngoingProcessByFileName(Controller.STORE_PROCESS, filename);
                linkedProcess.numberOfReceivedSuccessRequests.incrementAndGet();
            }

        }

        //REMOVE ACK OPERATION
        private void processRemoveACKOperation(String filename, int dPort) throws Exception {
            synchronized (Controller.lock)
            {
                ONGOING_PROCESS linkedProcess = Controller.getOngoingProcessByFileName(Controller.REMOVE_PROCESS, filename);
                int successResponses = linkedProcess.numberOfReceivedSuccessRequests.incrementAndGet();
            }
        }

        private void updateOngoingRequest(String process_type) throws Exception {
            synchronized (Controller.lock) {
                this.currentOngoingProcess = Controller.getOngoingProcessByFileName(process_type, this.currentOngoingProcess.fileName);
            }
        }

        private void checkIfSuccessStoreComplete() {
            this.finishedOngoingRequest = this.currentOngoingProcess.numberOfSentRequests == this.currentOngoingProcess.numberOfReceivedSuccessRequests.get();
        }

        private void checkIfSuccessRemoveComplete() {
            this.finishedOngoingRequest = this.currentOngoingProcess.numberOfSentRequests == this.currentOngoingProcess.numberOfReceivedSuccessRequests.get();
        }

        private void sendResponse(String response, PrintWriter outStream) {
            outStream.println(response);
            outStream.flush();
        }

        private void processDstoreDrop()
        {
            synchronized (Controller.lock)
            {
                int  i = 0;
                for (DSTORE_DATA eachDstore : Controller.dStores) {
                    if(eachDstore.dPort == Integer.parseInt(this.dPort)){
                        Controller.dStores.remove(i);
                        break;
                    }
                    i++;
                }
                Controller.currRFator--;
            }
        }

        private void processStoreDrop() throws Throwable
        {
            synchronized (Controller.lock)
            {
                String filename = this.currentOngoingProcess.fileName;
                Controller.removeOngoingProcess(this.currentOngoingProcess);
                Controller.updateDstoreStatuses(this.dstoresAffected, Controller.DSTORE_IDLE_STATUS);
                Controller.removeFileFromDstores(this.dstoresAffected, filename);
            }
        }

        private void processError(String errorMessage, PrintWriter outStream) {
            //errors that we need send
            ControllerLogger.getInstance().log(errorMessage);
            if (errorMessage.equals(Controller.ERROR_FILE_ALREADY_EXISTS) || errorMessage.equals(Controller.ERROR_FILE_DOES_NOT_EXIST) || errorMessage.equals(Controller.ERROR_NOT_ENOUGH_DSTORES)) {
                this.sendResponse(errorMessage, outStream);
                return;
            }

        }
    }

    //the rebalance operation will be on its own thread
    class REBALANCE_OPERATION implements Runnable
    {
        private List<DSTORE_DATA> testDstoreData;
        private List<DSTORE_DATA> realDstoreData;
        private List<HashMap<String, String>> filesToDistribute;
        private boolean newDstore;
        private int R;
        private int N;
        private int F;

        public REBALANCE_OPERATION(boolean newDstore){this.newDstore = newDstore; this.testDstoreData = Controller.dStores;}

        public void run()
        {
            this.setFilesToDistribute();
            this.setVariables();
            this.distributeEvenly();
            if(this.eachDstoreHasCorrectFileNumber()){
                // an element would look like this = (Dstore, {"remove" => [filesToRemove], "add" => [filesToAdd]})
                // where each files array would be List<HashMap<String, String>>
                // and each Dstore would be DSTORE_DATA
                List<HashMap<DSTORE_DATA, HashMap<String, List<HashMap<String, String>>>>> dStoresOperations = this.getRebalanceOperations();
                for (HashMap<DSTORE_DATA, HashMap<String, List<HashMap<String, String>>>> eachDstoreOperation : dStoresOperations) {
                    //todo figure out how to get the files to remove
                    //todo figure out how to get the files to add
                    //todo figure out how to get the dstore

                }



            } else if (this.newDstore) {
                //if we dont have the correct file number but we have a new Dstore then we should copy all the files to the new Dstore. TODO IMPLEMENT
            }

        }

        private void setVariables()
        {
            synchronized (Controller.lock)
            {
                this.R = Controller.rFactor;
                this.N = Controller.currRFator;
                this.F = filesToDistribute.size();
                this.realDstoreData = Controller.dStores;
            }
        }

        private void setFilesToDistribute()
        {
            synchronized (Controller.lock)
            {
                List<HashMap<String, String>> returnFilesArr = new ArrayList<HashMap<String, String>>();
                List<String> filesUsed = new ArrayList<String>();
                for (DSTORE_DATA eachDstore : Controller.dStores){
                 for (HashMap<String, String> eachFile : eachDstore.files) {
                     if(! filesUsed.contains(eachFile.get("filename"))){
                         filesUsed.add(eachFile.get("filename"));
                         returnFilesArr.add(eachFile);
                     }
                 }
             }
                this.filesToDistribute = returnFilesArr;
            }
        }

        private void distributeEvenly()
        {
            List<DSTORE_DATA> finalTestDstoreData = new ArrayList<>();
            List<DSTORE_DATA> testDstoreDataToUse = new ArrayList<>();
            testDstoreDataToUse = this.testDstoreData;

            //first clear the existing files to start fresh.
            for (DSTORE_DATA eachDstore : testDstoreDataToUse) {
                eachDstore.files = new ArrayList<>();

            }
            List<List<String>> result = new ArrayList<>();
            int dStorePartitions = this.testDstoreData.size();
            for (int i = 0; i < dStorePartitions; i++)
                finalTestDstoreData.add(testDstoreDataToUse.get(i));

            Iterator<HashMap<String, String>> iterator = this.filesToDistribute.iterator();
            for(int i = 0; iterator.hasNext(); i++)
                finalTestDstoreData.get(i % dStorePartitions).addFile(iterator.next().get("filename"), iterator.next().get("filesize"));

            this.testDstoreData = finalTestDstoreData;
        }

        private boolean eachDstoreHasCorrectFileNumber()
        {
            int lowerBound = (int) Math.floor((this.R * this.F) / this.N);
            int higherBound = (int) Math.ceil((this.R * this.F) / this.N);

            for (DSTORE_DATA eachDstore : this.testDstoreData) {
                int fileNumber = eachDstore.files.size();
                if(! (lowerBound <= fileNumber && fileNumber >= higherBound))
                    return false;
            }

            return true;
        }

        private List<HashMap< DSTORE_DATA, HashMap<String, List<HashMap<String, String>>>>> getRebalanceOperations()
        {
            List<HashMap<DSTORE_DATA, HashMap<String, List<HashMap<String, String>>>>> rtrnRebalanceArr = new ArrayList<>();

            for (DSTORE_DATA eachTestDstore : this.testDstoreData) {
                HashMap<DSTORE_DATA, HashMap<String, List<HashMap<String, String>>>> dStoreOperation = new HashMap<>();
                HashMap<String, List<HashMap<String, String>>> operations = new HashMap<>();
                DSTORE_DATA realDstoreDataSave = null;
                List<HashMap<String, String>> filesToRemove = new ArrayList<>();
                List<HashMap<String, String>>filesToAdd = new ArrayList<>();
                List<String> commonFiles = new ArrayList<>();

                //take the common files
                for (DSTORE_DATA eachRealDstore : this.realDstoreData){
                    if(eachTestDstore.dPort == eachRealDstore.dPort) {
                        for (HashMap<String, String> eachRealFile : eachRealDstore.files) {
                            if(eachRealDstore.hasFile(eachRealFile.get("filename"))){
                                realDstoreDataSave = eachRealDstore;
                                commonFiles.add(eachRealFile.get("filename"));
                            } else {
                                filesToRemove.add(eachRealFile);
                            }
                        }
                        break;
                    }
                }
                //now add the new files
                for (HashMap<String, String> eachTestFile : eachTestDstore.files) {
                    if(! commonFiles.contains(eachTestFile.get("filename"))){
                        filesToAdd.add(eachTestFile);
                    }
                }
                operations.put("remove", filesToRemove);
                operations.put("add", filesToAdd);
                dStoreOperation.put(realDstoreDataSave, operations); //todo can it really be null at this point?
                rtrnRebalanceArr.add(dStoreOperation);

            }

            return rtrnRebalanceArr;
        }

        //todo figure out how to write the update functions for each Dstore in an efficient way.

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
    public Controller(int cPort, int rFactor, int timeoutPer, int timeoutForNewReb) throws Exception {
        Controller.cPort = cPort;
        Controller.rFactor = rFactor;
        Controller.timeoutPer = timeoutPer;
        Controller.timeoutForNewReb = timeoutForNewReb;
        this.initialiseSystem();
    }

    /**
     * Main function, Used for starting the Controller server
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String cPort = args[0];
        String rFactor = args[1];
        String timeoutPer = args[2];
        String timeoutForNewReb = args[3];
        System.out.println("Starting server...");
        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        Controller sysStart = new Controller(Integer.parseInt(cPort), Integer.parseInt(rFactor), Integer.parseInt(timeoutPer), Integer.parseInt(timeoutForNewReb));
    }


    /**
     * The entrypoint of the program when the Controller starts
     *
     * @return void
     * @throws Exception
     */
    private void initialiseSystem() throws Exception {
        //setup the port to listen for incoming requests from client/Dstores
        this.socket = new ServerSocket(this.cPort);
        System.out.println("Controller Server started. Listening to requests.");
        //wait for incoming connections
        this.waitForRequests();
    }

    /**
     * Entrypoint for each request that comes in the Controller Server
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    private void waitForRequests() throws Exception {
        for (; ; ) {
            try {
                Socket client = Controller.socket.accept();
                //create new thread for the ongoing process.
                Thread request = new Thread(new REQUEST_THREAD(client));
                request.start();
            } catch (Throwable e) {
                System.out.println("UNEXPECTED CONTROLLER ERROR : " + (e.getMessage() != null ? e.getMessage() : e.toString()));
            }
        }
    }

    /**
     * Get ongoing process by the given file name.
     *
     * @param process_type String
     * @param filename String
     * @return void | ONGOING_PROCESS
     * @author Andrei123f(ap4u19@soton.ac.uk)
     * @throws Exception | void
     */
    static protected ONGOING_PROCESS getOngoingProcessByFileName(String process_type, String filename) throws Exception {
        for (ONGOING_PROCESS currOngoingProcess : Controller.ongoingProcesses) {
            if (currOngoingProcess.processType.equals(process_type) && currOngoingProcess.fileName.equals(filename)) {
                return currOngoingProcess;
            }
        }
        throw new Exception("Unknown File name. Requested : " + filename);

    }

    /**
     * Remove Ongoing Process from the Controller list.
     *
     * @param processToRemove ONGOING_PROCESS
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    static private void removeOngoingProcess(ONGOING_PROCESS processToRemove) {
        int i = 0;
        for (ONGOING_PROCESS currOngoingProcess : Controller.ongoingProcesses) {
            if (currOngoingProcess == processToRemove) {
                Controller.ongoingProcesses.remove(i);
                break;
            }
            i++;
        }
    }

    /**
     * Add the file to the given Dstore list. Will be used in the STORE Operation to keep track of the files.
     *
     * @param dstores List<DSTORE_DATA>
     * @param filename String
     * @param filesize String
     * @author Andrei123f(ap4u19@soton.ac.uk)
     * @throws Throwable | void
     */
    static private void addFileToDstores(List<DSTORE_DATA> dstores, String filename, String filesize) throws Throwable
    {
        for (DSTORE_DATA eachDstore : dstores) {
            if(! eachDstore.hasFile(filename))
                eachDstore.addFile(filename, filesize);
        }
    }

    /**
     * Remove the file from the given Dstore list
     *
     * @param dstores List<DSTORE_DATA>
     * @param filename String
     * @author Andrei123f(ap4u19@soton.ac.uk)
     * @throws Throwable | void
     */
    static void removeFileFromDstores(List<DSTORE_DATA> dstores, String filename) throws Throwable
    {
        for (DSTORE_DATA eachDstore : dstores) {
            eachDstore.removeFile(filename);
        }
    }

    /**
     * Update the status of the given Dstore List
     *
     * @param dstores List<DSTORE_DATA>
     * @param status String
     * @author Andrei123f(ap4u19@soton.ac.uk)
     * @throws Throwable | void
     */
    static void updateDstoreStatuses(List<DSTORE_DATA> dstores, String status) throws Throwable
    {
        for (DSTORE_DATA eachDstore : dstores) {
            eachDstore.updateStatus(status);
        }
    }

    /**
     * Helper function for printing out the current Ongoing processes.
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    static void printOngoingProcesses()
    {
        for(ONGOING_PROCESS eachProcess : Controller.ongoingProcesses) {
            System.out.println("===========================================================================");
            System.out.println("PROCESS TYPE : " + eachProcess.processType);
            System.out.println("PROCESS FILE : " + eachProcess.fileName);
            System.out.println("PROCESS SENT REQUESTS : " + eachProcess.numberOfSentRequests);
            System.out.println("PROCESS RECEIVED REQUESTS : " + eachProcess.numberOfReceivedSuccessRequests.get());

        }
    }
    /**
     * Helper function for printing out the current data from the Distributed Storage System.
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    static void printDstoreData()
    {
        System.out.println("R Factor : " + Controller.rFactor);
        System.out.println("Current N factor : " + Controller.currRFator);
        for (DSTORE_DATA eachDstore : Controller.dStores) {
            System.out.println("===========================================================================");
            System.out.println("DSTORE PORT : " + eachDstore.dPort);
            System.out.println("DSTORE STATUS : " + eachDstore.status);
            System.out.println("DSTORE FILES : ");

            if(eachDstore.files.size() != 0) {
                int i = 0;

                for (HashMap<String, String> eachFile : eachDstore.files) {
                    System.out.println("FILE#" + i + " NAME : " + eachFile.get("filename"));
                    System.out.println("FILE#" + i + " SIZE : " + eachFile.get("filesize"));
                    i++;
                }

            }else {
                System.out.println("NO FILES");
            }


        }
    }

    /**
     * Function to check if we have enough Dstores.
     * idleStatus is a flag to check if we have enough Dstores && have them in the idle status.
     *
     * @param idleStatus boolean
     * @author Andrei123f(ap4u19@soton.ac.uk)
     * @throws Exception | void
     */
    private static void checkIfEnoughDstores(boolean idleStatus) throws Exception {
        int dStores = idleStatus ? 0 : Controller.currRFator;

        //for store operation we need at least R dStores in the "idle status"
        if (idleStatus) {
            for (DSTORE_DATA eachDstore : Controller.dStores) {
                if (eachDstore.status.equals(Controller.DSTORE_IDLE_STATUS)) {
                    dStores++;
                }
            }
        }

        boolean result = Controller.currRFator >= Controller.rFactor;

        if (!result) {
            throw new Exception(Controller.ERROR_NOT_ENOUGH_DSTORES);
        }

    }

    /**
     * Function to check if the Dstore Connection still exists && update the index after we receive a success response from the current operation.
     *
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    private static void checkIfDstoreConnectionExists()
    {
        int i = 0;
        List<Controller.DSTORE_DATA> finalList = new ArrayList<>();
        for (DSTORE_DATA eachDstore : Controller.dStores) {
            int currPort = eachDstore.dPort;
            boolean connAlive = true;
            try {
                Socket ignored = new Socket("localhost", currPort);
            } catch (Throwable e) {
                connAlive = false;
            } finally {
                if(! connAlive) {
                    System.out.println("Connection to Dstore with port : "  + currPort + "  dropped.");
                    System.out.println("Updating index ...");
                    Controller.currRFator--;
                    System.out.println("Index updated. curr N factor : " + Controller.currRFator);
                    System.out.println("R factor : " + Controller.rFactor);
                } else {
                    finalList.add(eachDstore);
                }
            }
        i++;
        }
        Controller.dStores = finalList;
    }

    /**
     * Function to check if at least 1 Dstore has the file.
     *
     * @param filename String
     * @return boolean
     */
    private static boolean checkIfFileExists(String filename) {
        for (DSTORE_DATA eachDstore : Controller.dStores) {
            if (eachDstore.hasFile(filename)) {
                return true;
            }
        }
        return false;
    }

//todo implement rebalance operations when everything else if finished

}
