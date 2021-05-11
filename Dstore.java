import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
The Dstore class.

@author Andrei Popa(ap4u19@soton.ac.uk)
 */
public class Dstore {
    /* Define constants start */

    //expected incoming requests from Controller
    private static final String REMOVE_OPERATION = "REMOVE";


    //expected incoming requests from client
    private static final String STORE_OPERATION = "STORE";
    private static final String LOAD_DATA_OPERATION = "LOAD_DATA";

    //expected outgoing requests to Controller
    private static final String STORE_ACK = "STORE_ACK";
    private static final String REMOVE_ACK = "REMOVE_ACK";

    //expected outgoing requests to Client
    private static final String ACK = "ACK";

    //send to constants
    private static final String CONTROLLER_TARGET = "CONTROLLER";
    private static final String CLIENT_TARGET = "CLIENT";

    /* Define constants end */

    public int dPort;//the port of the dStore that the controller with communicate with
    public static int cPort; //the port of the controller
    public int timeout; //the timeout
    public static String folder_path; //the file_folder
    public static String currentOperation; //for updating the index for that specific DStore to reflect the process that is undertaken
    public static List<FILE> files = new ArrayList<FILE>(); //for keeping track of the files that each DStore has
    private static ServerSocket socket;//the socket where all the communications will happen(both with Controller and Client)
    private static Socket controllerSocket;
    static private Object lock = new Object();//for thread locking


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
                case Dstore.REMOVE_OPERATION:
                    this.operation = Dstore.REMOVE_OPERATION;
                    this.prepareRemoveOperation(segments);
                    break;
                case Dstore.LOAD_DATA_OPERATION:
                    this.operation = Dstore.LOAD_DATA_OPERATION;
                    this.prepareLoadDataOperation(segments);
                    break;

                case Dstore.STORE_OPERATION:
                    this.operation = Dstore.STORE_OPERATION;
                    this.prepareStoreOperation(segments);
                    break;

            }
        }

        private void prepareRemoveOperation(String[] requestSegments) {
            //expected request : REMOVE filename
            if (requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareLoadDataOperation(String[] requestSegments) {
            //expected request : LOAD_DATA filename
            if (requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareStoreOperation(String[] requestSegments) {
            //expected request : STORE filename filesize
            if (requestSegments.length != 3) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
            this.arguments.put("filesize", requestSegments[2]);

        }

    }

    static class FILE {
        public String filename;
        public String filesize;
        public InputStream fileInputStream;

        public FILE(String filename, String filesize) {
            this.filename = filename;
            this.filesize = filesize;
        }

        public void setFileInputStream(InputStream fileInputStream) {
            this.fileInputStream = fileInputStream;
        }
    }

    class REQUEST_THREAD implements Runnable {
        private Socket socketToOutput;//would be either Controller or Dstore

        //for text messages
        private BufferedReader textInStream;
        private PrintWriter textOutStream;
        private PrintWriter outTextStreamController;


        //for file transfers
        private OutputStream fileOutStream;
        private InputStream fileInStream;


        private String request_type;

        public REQUEST_THREAD(Socket socket) {
            this.socketToOutput = socket;
        }

        public void run() {
            try {
                this.textInStream = new BufferedReader(new InputStreamReader(this.socketToOutput.getInputStream()));
                this.textOutStream = new PrintWriter(this.socketToOutput.getOutputStream());


                this.fileOutStream = this.socketToOutput.getOutputStream();


                String request;
                request = textInStream.readLine();

                System.out.println("Incoming request : " + request);
                INCOMING_REQUEST formattedRequest = new INCOMING_REQUEST(request);

                if (formattedRequest.invalidOperation || formattedRequest.invalidArguments) {
                    throw new Exception("invalid request: " + request); //todo change here
                }

                String response;
                this.request_type = formattedRequest.operation;

                switch (formattedRequest.operation) {
                    case Dstore.REMOVE_OPERATION:
                        response = this.processRemoveOperation(formattedRequest.arguments.get("filename"));
                        this.sendTextResponse(response, new PrintWriter(Dstore.controllerSocket.getOutputStream()));
                        break;
                    case Dstore.LOAD_DATA_OPERATION:
                        FILE fileToSend = this.processLoadDataOperation(formattedRequest.arguments.get("filename"));
                        this.sendFileResponse(fileToSend, this.fileOutStream);
                        break;
                    case Dstore.STORE_OPERATION:
                        response = this.processStoreOperation(formattedRequest.arguments.get("filename"), formattedRequest.arguments.get("filesize"));
                        System.out.println("Sending store ACK response to controller. : " + response);
                        this.sendResponseToController(response);
                        System.out.println("ACK response to controller sent.");
                        break;

                }

                this.socketToOutput.close();
                System.out.println("Request for operation " + request_type + " terminated Successfully. Closing thread...");

            } catch (Throwable e) {
                System.out.println("Exception : " + (e.getMessage() == null ? e.toString() : e.getMessage()) + " for request type : " + this.request_type + ". Closing thread...");
                this.handleError(e.getMessage(), textOutStream);
            }
        }


        private String processRemoveOperation(String filename) throws Exception {
            synchronized (Dstore.lock) {
                FILE fileData = Dstore.getFile(filename);
                String rtrn_request = null;

                File file = new File(Dstore.folder_path + fileData.filename);
                //delete the file
                if (file.delete()) {
                    System.out.println("File" + filename + "deleted successfully");
                    rtrn_request = Dstore.REMOVE_ACK + " " + filename;
                } else {
                    throw new Exception("The file does not exist");
                }

                //remove file from the list
                Dstore.removeFile(filename);

                return rtrn_request;
            }
        }

        private FILE processLoadDataOperation(String filename) throws Exception {
            synchronized (Dstore.lock) {
                FILE fileData = Dstore.getFile(filename);

                File file = new File(Dstore.folder_path + fileData.filename);
                InputStream inputFileStream;

                if (file.exists()) {
                    inputFileStream = new FileInputStream(file);
                    fileData.setFileInputStream(inputFileStream);
                } else {
                    throw new Exception("The file does not exist");
                }

                return fileData;
            }
        }

        private String processStoreOperation(String filename, String filesize) throws Exception {

            System.out.println("Sending ACK Text response to client...");
            //send ACK to client
            this.sendTextResponse(Dstore.ACK, this.textOutStream);
            //prepare success response to be sent to Controller.
            String rtrn_request = Dstore.STORE_ACK + " " + filename;

            System.out.println("Copying file...");

            File newFile = new File(Dstore.folder_path + filename);

            if(newFile.createNewFile()) {

                FileOutputStream fileOutputStream = new FileOutputStream(newFile);

                int fileSizeInt = Integer.parseInt(filesize);

                System.out.println("file size : " + fileSizeInt);

                System.out.println("reading bytes");
                this.fileInStream = this.socketToOutput.getInputStream();
                System.out.println("reading bytes22222323232");
                byte[] bytes = this.fileInStream.readNBytes(fileSizeInt);
                byte[] buf = new byte[1000];


                System.out.println("writing bytes");
                fileOutputStream.write(bytes);
                fileOutputStream.close();

                System.out.println("File copied.");
                //add the file for internal use
                Dstore.addFile(filename, filesize);

                return rtrn_request;
            }

            return rtrn_request;
        }


        private void handleError(String errorMessage, PrintWriter outStream) {

        }

        private void sendTextResponse(String message, PrintWriter outStream) {
            outStream.print(message);
            outStream.flush();
        }

        private void sendFileResponse(FILE fileData, OutputStream outPutS) throws Exception {
            int fileSize = Integer.parseInt(fileData.filesize);

            byte[] bytes = new byte[16 * 1024];
            InputStream fileInputStream = fileData.fileInputStream;
            int count = 0;
            while ((count = fileInputStream.read(bytes)) > 0) {
                outPutS.write(bytes, 0, count);
            }
            outPutS.close();
        }

        private void sendResponseToController(String message) throws Exception {
            synchronized (Dstore.lock) {
                Dstore.controllerSocket = new Socket("localhost", Dstore.cPort);
                PrintWriter outPutStream = new PrintWriter(Dstore.controllerSocket.getOutputStream());
                this.sendTextResponse(message, outPutStream);
                outPutStream.close();
            }

        }

        private void sendTextResponseToClient(String message) throws Exception {
            synchronized (Dstore.lock) {

            }
        }


    }


    public static void main(String[] args) throws Exception {
        String port = args[0];
        String cPort = args[1];
        String timeoutPer = args[2];
        String fileFolder = args[3];
        System.out.println("Starting database server...");
        Dstore sysStart = new Dstore(Integer.parseInt(port), Integer.parseInt(cPort), Integer.parseInt(timeoutPer), fileFolder);
    }


    public Dstore(int port, int cPort, int timeout, String file_folder) {
        this.dPort = port;
        Dstore.cPort = cPort;
        this.timeout = timeout;
        Dstore.folder_path = file_folder;
        try {
            this.initialiseSystem();

        } catch (Exception e) {

        }
    }


    private void initialiseSystem() throws Exception {
        //setup the port to listen for incoming requests from Controller/Client
        Dstore.socket = new ServerSocket(this.dPort);
        Dstore.controllerSocket = new Socket("localhost", Dstore.cPort);
        System.out.println("Dstore Database Server started. Joining Server ...");
        //connect to the Controller
        this.connectToServer();
        System.out.println("Connected to server. Listening to requests.");
        //wait for incoming connections
        this.waitForRequests();

    }

    private void connectToServer() throws Exception {
        String request = Controller.JOIN_OPERATION + " " + this.dPort;
        PrintWriter outPutStream = new PrintWriter(Dstore.controllerSocket.getOutputStream());
        outPutStream.print(request);
        outPutStream.flush();
        outPutStream.close();
    }

    private void waitForRequests() throws Exception {
        for (; ; ) {
            Socket client = Dstore.socket.accept();
            //create new thread for the ongoing process.
            Thread request = new Thread(new REQUEST_THREAD(client));
            request.start();
        }
    }

    private static FILE getFile(String filename) throws Exception {
        for (FILE eachFile : Dstore.files) {
            if (eachFile.equals(filename)) {
                return eachFile;
            }
        }

        throw new Exception("File not found");
    }

    private static void addFile(String filename, String filesize) {
        Dstore.files.add(new FILE(filename, filesize));
    }

    private static void removeFile(String filename) {
        int i = 0;
        for (FILE eachFile : Dstore.files) {
            if (eachFile.filename.equals(filename)) {
                Dstore.files.remove(i);
            }
            i++;
        }
    }
}
