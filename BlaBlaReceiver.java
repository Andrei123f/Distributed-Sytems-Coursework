import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class BlaBlaReceiver
{


    public static void main(String[] args)
    {
        try {
            ServerSocket ss = new ServerSocket(4020);
            for(;;) {
                try{
                    Socket client = ss.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line;
                    while((line = in.readLine()) != null)
                        System.out.println(line+" received");
                    client.close();
                } catch (Exception e) {
                    System.out.print("An unexpected exception has been thrown inside the strange for loop : " + e.getMessage());
                }
            }



        } catch (Exception e) {
            System.out.print("An unexpected exception has been thrown : " + e.getMessage());
        }

    }
}
