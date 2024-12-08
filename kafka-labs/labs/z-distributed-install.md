# Setting up a Distributed Kafka Cluster 

ssh into server1
- install zookeper
- start zookeeper 

- untar kafka 
- edit  ~/apps/kafka/conf/server.properties 
     broker.id=0
     # use internal ip for zk
     zookeeper.connect=ZK_SERVER_INTERNAL:2181
     
- start kafka 

- setup kafka manager
- register kafka
    - make sure zk server is set to : ZK_SERVER_INTERNAL:2181
    
- copy kafka to server2
    scp -r ~/kafka   server2:
    
- ssh into server2 
    - edit  ~/apps/kafka/conf/server.properties
        broker.id=1
    
    - start kafka on server2 

    -  now look at kafka manager, you should see server2 joined and 2 brokers
    
- copy kafka to server3
    scp -r ~/kafka   server3:
    
- ssh into server3 
    - edit  ~/apps/kafka/conf/server.properties
        broker.id=2
    
    - start kafka on server3

    -  now look at kafka manager, you should see server3 joined and 3 brokers
    
