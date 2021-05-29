# Distributed-Sytems-Coursework
This coursework was part of the Distributed Systems & Networks module.
We had to design & implement a distributed storage system using Java. The system has 2 main components : 
  - Controller : The entrypoint of every request that comes to the server && what handles all the logic
  - Dstore : The representation of a remote storage system.
A controller needs at least R remote storages(Dstores) connected to the system in order to process client requests.

Client requests may include : 
 - STORE, when the client wants to store a new file into the system. In this case the Controller will select R available Dstores and store the file into them.
 - LOAD, when client wants to get a stored file from the sysyem. In this case the Controller will select the first Dstore that has that file and share the file with the Client.
 - REMOVE, when the client wants to delete a stored file from the system. In this case the Controller will send remove requests to all Dstores to delete that file.
 - LIST, when the client wants to know the list of files present in the system. In this case the Controller will send a list of all present files from the system.

Each request has its own thread, meaning that the system can support multiple client requests(possibly with the same operation) at once.
For development purposes, I have kept the server to "localhost" in both Controller and Dstore.
Below you can find a diagram of the designed system.
![image](https://user-images.githubusercontent.com/31124236/120066517-ca6cde00-c06e-11eb-8993-4514aea641ae.png)
