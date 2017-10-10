################# Chord Implementation of Distributed Hash Table ################

### Concept: 
Implementating a DHT to store database with help of ID Space partitioning using a Chord Ring for data routing.

### Implementation:
* The application is launched on 5 different AVDs each with a specified port.
* Each application has a server port which is constantly active to receive messages and a client which sends messages to the other AVDs.
* Messages are sent over the devices using TCP protocol.
* The Client and Server ports are implemented using Java Sockets and Java AsycTasks.
* Messages received are stored in a key-value local Map for every AVD.
* The Nodes created using AVDs are formulated in a Ring based structure as a Chord Ring.
* Every Data input (key-value) is stored at a specific Node based on the Hash value of its key generated using SHA-1.
* The application can handle node departure and node rejoins.
* During Data retrieval Nodes follow the Ring sturcture to send to request to the appropriate node storing the data.
* Data can be queried based on its key from any Node. 

### Link to the Code files:
https://github.com/Sumedh0192/Distributed-Systems/tree/master/SimpleDht/app/src/main/java/edu/buffalo/cse/cse486586/simpledht/

### Link to official project description:
https://github.com/Sumedh0192/Distributed-Systems/blob/master/SimpleDht/PA%203%20Specification.pdf
