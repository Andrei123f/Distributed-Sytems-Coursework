import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Dstore class - this will be the node of the Distributed Storage.
 * A singleton design pattern will be used for this class, where each property will be accessible && dynamically changed by the request threads.
 *
 * @author Andrei23f(ap4u19@soton.ac.uk)
 */
class Dstore{
    static int port = 0;
    static int cport = 0;
    static int timeout = 0;
    static String file_folder = null;
    // (fileName, fileSize) is a tuple from the file_details hashtable
    static Hashtable<String, Integer> file_details = new Hashtable<>();
    static ServerSocket ss;
    static Socket socket_to_controller;

    private static Object lock = new Object();//for dealing with Thread race conditions
    /**
     * The class that will validate && format each request that comes from Client / Controller.
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
            DstoreLogger.getInstance().log("Incoming request from client : " + request);
            //split the request by spaces
            String segments[] = request.split(" ");
            //get the request type
            String request_type = segments[0];
            //decide if the the request type is correct or not
            switch (request_type) {
                case Protocol.REMOVE_TOKEN:
                    this.operation = Protocol.REMOVE_TOKEN;
                    this.prepareRemoveOperation(segments);
                    break;
                case Protocol.LOAD_DATA_TOKEN:
                    this.operation = Protocol.LOAD_DATA_TOKEN;
                    this.prepareLoadDataOperation(segments);
                    break;

                case Protocol.STORE_TOKEN:
                    this.operation = Protocol.STORE_TOKEN;
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

    /**
     * The class that will be only used for Controller requests.
     *
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    public class CONTROLLER_THREAD implements Runnable
    {
        private Socket socketTo_controller;

        private OutputStream outFileStream_controller;
        private InputStream inFileStream_controller;

        private BufferedReader inTextStream_controller;
        private PrintWriter outTextStream_controller;

        public CONTROLLER_THREAD (Socket socketTo_controller) {this.socketTo_controller = socketTo_controller;}

        public void run()
        {
            try
            {
                this.outFileStream_controller = this.socketTo_controller.getOutputStream();
                this.inFileStream_controller = this.socketTo_controller.getInputStream();

                this.inTextStream_controller = new BufferedReader(new InputStreamReader(this.inFileStream_controller));
                this.outTextStream_controller = new PrintWriter(new OutputStreamWriter(this.outFileStream_controller));

                //join the controller server
                this.outTextStream_controller.println(Protocol.JOIN_TOKEN + " " + Dstore.port);
                this.outTextStream_controller.flush();

                //waiting for commands from Controller
                String line;
                while((line = this.inTextStream_controller.readLine()) != null)
                {
                    System.out.println("Incoming request from Controller: " + line);
                    DstoreLogger.getInstance().log("Incoming request from Controller: " + line);
                    INCOMING_REQUEST formattedRequest = new INCOMING_REQUEST(line);

                    if(formattedRequest.invalidOperation || formattedRequest.invalidArguments)
                        throw new Exception("Invalid Request from Controller" + line);

                    switch (formattedRequest.operation) {
                        case (Protocol.REMOVE_TOKEN) :
                            this.processRemoveOperation(formattedRequest.arguments.get("filename"));
                            break;
                    }

                }


            }catch (Throwable e) {
                String error  = ("Error when receiving request from Controller: " + (e.getMessage() != null ? e.getMessage() : e.toString()));
                DstoreLogger.getInstance().log(error);
            }
        }

        private void processRemoveOperation(String filename)
        {
            File file = new File(Dstore.file_folder + File.separator + filename);
            String responseTo_controller;
            if(file.delete()) {
                Dstore.file_details.remove(filename);
                responseTo_controller = Protocol.REMOVE_ACK_TOKEN + " " + filename;

            } else {
                responseTo_controller = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename;
            }
            this.outTextStream_controller.println(responseTo_controller);
            this.outTextStream_controller.flush();
        }

    }

    /**
     * The class that will be only used for Controller/Client requests.
     *
     * @author Andrei123f(ap4u19@soton.ac.uk)
     */
    class CLIENT_THREAD implements Runnable
    {
        private Socket socketTo_client;

        //for Client communication
        private OutputStream outFileStream_client;
        private InputStream inFileStream_client;

        private BufferedReader inTextStream_client;
        private PrintWriter outTextStream_client;

        //for Controller communication
        private OutputStream outFileStream_controller;
        private InputStream inFileStream_controller;

        private BufferedReader inTextStream_controller;
        private PrintWriter outTextStream_controller;


        public CLIENT_THREAD (Socket socket) {this.socketTo_client = socket;}

        public void run()
        {
            try
            {
                this.outFileStream_controller = socket_to_controller.getOutputStream();
                this.inFileStream_controller = socket_to_controller.getInputStream();
                this.inTextStream_controller = new BufferedReader(new InputStreamReader(this.inFileStream_controller));
                this.outTextStream_controller = new PrintWriter(new OutputStreamWriter(this.outFileStream_controller));

                this.outFileStream_client = this.socketTo_client.getOutputStream();
                this.inFileStream_client = this.socketTo_client.getInputStream();
                this.inTextStream_client = new BufferedReader(new InputStreamReader(this.inFileStream_client));
                this.outTextStream_client = new PrintWriter(new OutputStreamWriter(this.outFileStream_client));

                int bufLen;
                byte[] buffer = new byte[1000];

                while((bufLen = this.inFileStream_client.read(buffer)) != -1){
                    String request = new String(buffer, 0, bufLen);
                    request = request.replaceAll("\n","");
                    System.out.println("Incoming request from client : " + request);
                    INCOMING_REQUEST formattedRequest = new INCOMING_REQUEST(request);

                    if(formattedRequest.invalidOperation || formattedRequest.invalidArguments)
                        throw new Exception("Invalid Request from Client/Controller" + request);

                    switch (formattedRequest.operation) {
                        case (Protocol.STORE_TOKEN):
                            this.processStoreOperation(formattedRequest.arguments.get("filename"), formattedRequest.arguments.get("filesize"));
                            break;
                        case (Protocol.LOAD_DATA_TOKEN):
                            this.processLoadOperation(formattedRequest.arguments.get("filename"));
                            break;
                        case(Protocol.REMOVE_TOKEN):
                            this.processRemoveOperation(formattedRequest.arguments.get("filename"));
                            break;
                    }
                }

            } catch (Throwable e){
                String error = ("Error when receiving request from Client/Controller: " + (e.getMessage() != null ? e.getMessage() : e.toString()));
                DstoreLogger.getInstance().log(error);
            }

        }


        public void processStoreOperation(String filename, String filesize) throws Throwable
        {
            int fileSize = Integer.parseInt(filesize);
            File file = new File(file_folder + File.separator + filename);
            try {
                if (file.createNewFile()) {
                    System.out.println("Sending ACK response to client ...");
                    //send ack response to client
                    this.outTextStream_client.println(Protocol.ACK_TOKEN);
                    this.outTextStream_client.flush();
                    System.out.println("Sending ACK response to client - success");

                    FileOutputStream outFile = new FileOutputStream(file);
                    byte[] data;
                    System.out.println("Reading N bytes ...");
                    data = this.inFileStream_client.readNBytes(fileSize);
                    System.out.println("Reading N bytes - success");

                    System.out.println("Writing the file ...");
                    outFile.write(data);
                    outFile.close();
                    System.out.println("Writing the file - success");

                    System.out.println("Sending Store ACK response to Controller for file " + filename + " ...");
                    //send ack response to controller.
                    this.outTextStream_controller.println(Protocol.STORE_ACK_TOKEN + " " + filename);
                    this.outTextStream_controller.flush();
                    System.out.println("Sending Store ACK response to Controller for file " + filename + " ... - success");
                    file_details.put(filename, Integer.valueOf(fileSize));

                } else {
                    //todo should you close the socket client here?
                    System.out.println("File already exists.");
                    this.socketTo_client.close();
                    throw new Error("File already exists");
                }
            } catch (Throwable e) {
                throw new Error("Error when storing file : " + (e.getMessage() != null ? e.getMessage() : e.toString()));
            }
        }


        public void processLoadOperation(String filename) throws Throwable
        {
            synchronized (Dstore.lock) {
                try {
                    System.out.println("LOADING FILE OPERATION ...");
                    if (file_details.containsKey(filename)) {
                        File inputFile = new File(filename);
                        FileInputStream inf = new FileInputStream(Dstore.file_folder + File.separator + inputFile);
                        int buflen;
                        byte[] buf = new byte[1000];
                        System.out.println("LOADING FILE OPERATION started...");
                        while ((buflen = inf.read(buf)) != -1) {
                            this.outFileStream_client.write(buf, 0, buflen);
                        }
                        inf.close();
                        System.out.println("LOADING FILE OPERATION finished...");

                    } else {
                        //todo should you close the socket client here?
                        this.socketTo_client.close();
                        throw new Error("Dstore does not have the requested file : " + filename);

                    }


                } catch (Throwable e) {
                    throw new Error("Error when loading file : " + (e.getMessage() != null ? e.getMessage() : e.toString()));
                }
            }
        }

        public void processRemoveOperation(String filename) throws Throwable
        {
            System.out.println("Deleting file " + filename + " ...");
            File file = new File(Dstore.file_folder + File.separator + filename);
            String responseTo_controller;
            if(file.delete()) {
                System.out.println("Deleting file " + filename + " - finished");
                Dstore.file_details.remove(filename);
                responseTo_controller = Protocol.REMOVE_ACK_TOKEN + " " + filename;

            } else {
                //todo should you close the socket client here?
                responseTo_controller = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename;
            }
            System.out.println("Sending response to Controller... : " + responseTo_controller);
            this.outTextStream_controller.println(responseTo_controller);
            this.outTextStream_controller.flush();
            System.out.println("Sending response to Controller... : " + responseTo_controller + " - finished");
        }


    }

    /**
     * The entrypoint of the program when the Dstore node starts
     *
     * @throws Exception | void
     */
    public static void main (String[] args ) throws Throwable
    {
        Dstore sys = new Dstore();
        sys.main2(args);

    }

    public void main2(String[] args) throws IOException {
        try{
            System.out.println(args[0]+" "+args[1]+" "+args[2]+" "+args[3]);
            //dstore port
            port = Integer.parseInt(args[0]);
            //controller port
            cport = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            file_folder = args[3];
            File dstoreFolder = new File(file_folder);
            DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
            DstoreLogger.getInstance().log("Starting Dstore server ... on port" + port);
            if (!dstoreFolder.exists())
                if (!dstoreFolder.mkdir()) throw new RuntimeException("Cannot create dstore folder (folder absolute path: " + dstoreFolder.getAbsolutePath() + ")");
            try {
                try {
                    socket_to_controller = new Socket("localhost", cport);
                    //communication tools for controller <-> client
                    Thread controllerThread = new Thread(new CONTROLLER_THREAD(socket_to_controller));
                    controllerThread.start();
                } catch (Throwable e) {
                    String error = ("Unexpected System error when listening for Controller requests : " + (e.getMessage() != null ? e.getMessage() : e.toString()));
                    DstoreLogger.getInstance().log(error);

                }

                try {
                    ss = new ServerSocket(port);
                    for(;;) {
                        try {
                            if (socket_to_controller.isConnected()) {
                                System.out.println("Dstore is connected to the Controller server. Listening to Client Requests.");
                                Socket socket_to_client = ss.accept();
                                Thread clientThread = new Thread(new CLIENT_THREAD(socket_to_client));
                                clientThread.start();
                            } else {
                            }
                            System.out.println("Dstore is not connected to the Controller server.");

                        }catch (Throwable e){
                            String error = ("error"+e);
                            //DstoreLogger.getInstance().log(error);
                        }
                    }
                } catch (Throwable e){
                    String error = ("Unexpected System error when listening for Client requests : " + (e.getMessage() != null ? e.getMessage() : e.toString()));
                    //DstoreLogger.getInstance().log(error);
                }

            } catch (Throwable e){
                String error = ("Unexpected System error when listening for overall requests : " + (e.getMessage() != null ? e.getMessage() : e.toString()));
                //DstoreLogger.getInstance().log(error);
            }
        }catch(Throwable e){
            String error = ("Unexpected System error when initialising the server : " + (e.getMessage() != null ? e.getMessage() : e.toString()));
            //DstoreLogger.getInstance().log(error);
        }
    }
}