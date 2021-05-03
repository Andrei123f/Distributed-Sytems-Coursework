import java.io.*;
import java.net.*;



public class BlaBla
{

    public static void main(String[] args)
    {
        try {
            Socket socket = new Socket("localhost", 4020);
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            for (int i = 0; i < 10; i++) {
                out.println("TCP message " + i);
                out.flush();

               System.out.println("TCP message : " + i + " sent!");
               Thread.sleep(1000);
            }


        } catch (Exception e) {
            System.out.print("An unexpected exception has been thrown : " + e.getMessage());
        }




        String da = "dadadada";


        da+= "daboss";
     System.out.println("HELLO WORLD JAVA");
    }
    /*
    Distributed storage system
        - 1 Controller
        - N Dstores
        - multiple concurrent clients can send different requests
        - requests include store, load, list, remove
    App
        - each file is replicated R times over different Dstores
    Design Choices
        - Files are stores by Dstores
        - Controller only controls the client requests and keeps track of the index allocation of the files as well as the size of each stored file.
        - The client gets the files directly from Dstores
        -





     */
}
