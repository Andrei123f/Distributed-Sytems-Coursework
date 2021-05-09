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
public class Dstore
{
    /* Define constants start */

    //expected incoming requests from Controller
    private static final String REMOVE_OPERATION = "REMOVE";


    //expected incoming requests from client
    private static final String STORE_OPERATION = "STORE";
    private static final String LOAD_DATA_OPERATION = "LOAD_DATA";

    //expected outgoing requests to Controller
    private static final String STORE_ACK = "STORE_ACK";
    private static final String REMOVE_ACK = "REMOVE_ACK";

    //send to constants
    private static final String CONTROLLER_TARGET = "CONTROLLER";
    private static final String CLIENT_TARGET = "CLIENT";

    /* Define constants end */

    public int dPort;//the port of the dStore that the controller with communicate with
    public int cPort; //the port of the controller
    public int timeout; //the timeout
    public String folder_path; //the file_folder
    public String currentOperation; //for updating the index for that specific DStore to reflect the process that is undertaken
    public List<FILE> files = new ArrayList<FILE>(); //for keeping track of the files that each DStore has
    private ServerSocket socket;//the socket where all the communications will happen(both with Controller and Client)
    private Socket controllerSocket;


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
        private void prepareRemoveOperation(String[] requestSegments)
        {
            //expected request : REMOVE filename
            if(requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareLoadDataOperation(String[] requestSegments)
        {
            //expected request : LOAD_DATA filename
            if(requestSegments.length != 2) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
        }

        private void prepareStoreOperation(String[] requestSegments)
        {
            //expected request : STORE filename filesize
            if(requestSegments.length != 3) {
                this.invalidArguments = true;
                return;
            }
            this.arguments.put("filename", requestSegments[1]);
            this.arguments.put("filesize", requestSegments[2]);

        }

    }

    class FILE
    {
        public String filename;
        public String filesize;
        public InputStream fileInputStream;

        public FILE(String filename, String filesize) {this.filename = filename; this.filesize = filesize;}

        public void setFileInputStream(InputStream fileInputStream) {this.fileInputStream = fileInputStream;}
    }






    public static void main(String[] args) throws Exception
    {
        String port = args[0];
        String cPort = args[1];
        String timeoutPer = args[2];
        String fileFolder = args[3];
        System.out.println("Starting database server...");
        Dstore sysStart = new Dstore(Integer.parseInt(port), Integer.parseInt(cPort), Integer.parseInt(timeoutPer), fileFolder);
    }


    public Dstore(int port, int cPort, int timeout, String file_folder)
    {
        this.dPort = port;
        this.cPort = cPort;
        this.timeout = timeout;
        this.folder_path = file_folder;
        try {
            this.initialiseSystem();

        }catch (Exception e){

        }
    }


    private void initialiseSystem()  throws Exception
    {
        //setup the port to listen for incoming requests from Controller/Client
        this.socket = new ServerSocket(this.dPort);
        this.controllerSocket = new Socket("localhost", this.cPort);
        System.out.println("Dstore Database Server started. Joining Server ...");
        //connect to the Controller
        this.connectToServer();
        System.out.println("Connected to server. Listening to requests.");
        //wait for incoming connections
        this.waitForRequests();

    }

    private void connectToServer() throws Exception
    {
        String request = Controller.JOIN_OPERATION + " " + this.dPort;
        PrintWriter outPutStream = new PrintWriter(this.controllerSocket.getOutputStream());
        outPutStream.print(request);
        outPutStream.flush();
        outPutStream.close();
    }

    private void waitForRequests() throws Exception
    {
        for(;;) {
            Socket client = this.socket.accept();
            //for text requests
            BufferedReader inStream = new BufferedReader(new InputStreamReader(client.getInputStream()));
            PrintWriter outStream = new PrintWriter(client.getOutputStream());

            //for file requests
            OutputStream outFileStream = client.getOutputStream();
            InputStream inFileStream = client.getInputStream();

            String request;

            while((request = inStream.readLine()) != null) {
                System.out.println("Incoming request : " + request);
                INCOMING_REQUEST formattedRequest = new INCOMING_REQUEST(request);

                if(formattedRequest.invalidOperation || formattedRequest.invalidArguments)
                    throw new Exception("invalid request: " + request); //todo change here

                String response;

                switch (formattedRequest.operation) {
                    case Dstore.REMOVE_OPERATION:
                        response = this.processRemoveOperation(formattedRequest.arguments.get("filename"));
                        this.sendTextResponse(response, Dstore.CONTROLLER_TARGET, null);
                        break;
                    case Dstore.LOAD_DATA_OPERATION:
                        FILE fileToSend = this.processLoadDataOperation(formattedRequest.arguments.get("filename"));
                        this.sendFileResponse(fileToSend, Dstore.CLIENT_TARGET, outFileStream);
                        break;
                    case Dstore.STORE_OPERATION:
                        response = this.processStoreOperation(formattedRequest.arguments.get("filename"), formattedRequest.arguments.get("filesize"), inFileStream);
                        this.sendTextResponse(response, Dstore.CONTROLLER_TARGET, null);
                        break;
                }
            }
        }
    }

    private String processRemoveOperation(String filename) throws Exception
    {
        FILE fileData = this.getFile(filename);
        String rtrn_request = null;

        File file = new File(this.folder_path + fileData.filename);
        //delete the file
        if(file.delete()) {
            System.out.println("File" + filename +  "deleted successfully");
            rtrn_request = Dstore.REMOVE_ACK + " " + filename;
        } else {
            throw new Exception("The file does not exist");
        }

        //remove file from the list
        this.removeFile(filename);

        return rtrn_request;
    }


    private FILE processLoadDataOperation(String filename) throws Exception
    {
        FILE fileData = this.getFile(filename);

        File file  = new File(this.folder_path + fileData.filename);
        InputStream inputFileStream;

        if(file.exists()) {
            inputFileStream = new FileInputStream(file);
            fileData.setFileInputStream(inputFileStream);
        } else {
            throw new Exception("The file does not exist");
        }

        return fileData;
    }


    private String processStoreOperation(String filename, String filesize, InputStream inFileStream) throws Exception
    {
        String rtrn_request = Dstore.STORE_ACK + " " + filename;

        OutputStream fileOutputStream = new FileOutputStream(this.folder_path + filename);
        byte[] bytes = new byte[16 * 1024];
        int count;

        while ((count = inFileStream.read(bytes)) > 0) {
            fileOutputStream.write(bytes, 0, count);
        }
        fileOutputStream.close();
        inFileStream.close();
        //add the file for internal use
        this.addFile(filename, filesize);

        return rtrn_request;
    }





    private FILE getFile(String filename) throws Exception
    {
        for (FILE eachFile : this.files) {
            if(eachFile.equals(filename)) {
                return eachFile;
            }
        }

        throw new Exception("File not found");
    }

    private void addFile(String filename, String filesize)
    {
        this.files.add(new FILE(filename, filesize));
    }

    private void removeFile(String filename)
    {
        int  i = 0;
        for (FILE eachFile : this.files) {
            if(eachFile.filename.equals(filename)){
                this.files.remove(i);
            }
            i++;
        }
    }

    private void sendTextResponse(String response, String sendTo, PrintWriter outPutS) throws Exception
    {
        PrintWriter outputStream = (sendTo.equals(Dstore.CONTROLLER_TARGET)) ? new PrintWriter(this.controllerSocket.getOutputStream()) : outPutS;
        outputStream.print(response);
        outputStream.flush();
        outputStream.close();
    }

    private void sendFileResponse (FILE fileData, String sendTo, OutputStream outPutS) throws Exception
    {
        OutputStream outputStream = (sendTo.equals(Dstore.CONTROLLER_TARGET)) ? this.controllerSocket.getOutputStream() : outPutS;
        int fileSize = Integer.parseInt(fileData.filesize);

        byte[] bytes = new byte[16 * 1024];
        InputStream fileInputStream = fileData.fileInputStream;
        int count = 0;
        while ((count = fileInputStream.read(bytes)) > 0) {
            outputStream.write(bytes, 0, count);
        }
        outputStream.close();
    }

}
