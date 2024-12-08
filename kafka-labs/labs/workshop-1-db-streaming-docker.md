<link rel='stylesheet' href='../assets/css/main.css'/>

[<< back to main index](../README.md)

# Workshop-1: Kafka Use Case: Connecting to MySQL database using Kafka connect

### Overview
We will try to connect with MySql Database over Kafka-connect

### Depends On
- Docker > 1.11 version

### Run time
- 1 hr

## Step 1: Start Kafka and ZooKeeper

Since we have mentioned that docker are getting used, so let's jump right into it. 
We will start the zookeeper using docker

```bash
docker run -d -it --rm --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 debezium/zookeeper:1.0
```

To see the logs of the docker container

```bash
docker logs -f zookeeper
```

Similarly, we are going to start kafka in another container

```bash
docker run -d -it --rm --name kafka -p 9092:9092 --link zookeeper:zookeeper debezium/kafka:1.0
```

Notes: `-p` binds the ports of`host:container`

## Step 2: Start a MySQL database

Now that we have the Kafka and Zookeeper ready, it's time for us to start the MySQL database container

Run the following command and start a docker container with MySQL DB running in it.

```bash
docker run -d \
-it --rm --name mysql \
-p 3306:3306 \
-e MYSQL_ROOT_PASSWORD=kafkalabs \
-e MYSQL_USER=labuser \
-e MYSQL_PASSWORD=lablpw \
debezium/example-mysql:1.0
```

Give it some time to start the container properly and you can keep checking the logs and wait for it to get stable with running the socket at port 3306.

For the logs:
```bash
docker logs -f mysql
```
Note: `-e`: Environment variables set to start the containers.

## Step 3: Start a MySQL client for CLI (Command line interface)


If you wish to keep looking at the container logs, then you can start a new termial and use another docker image `mysql:5.7`, any version less than 5.7 might not work.

So, open a new terminal and start the container 

```bash
docker run -d \
-it --rm --name mysqlclient \
--link mysql \
--rm mysql:5.7 \
sh -c 'exec mysql -h"$MYSQL_PORT_3306_TCP_ADDR" \
 -P"$MYSQL_PORT_3306_TCP_PORT" -uroot \
 -p"$MYSQL_ENV_MYSQL_ROOT_PASSWORD"'
```
So many things are happening here:

- `link` : We are connecting this container with the previous mysql container
- `sh` : we are just executing a shell command to start the mysql command line using the user we created in the previous session


**QUESTION** for you: From where the variable `MYSQL_ENV_MYSQL_ROOT_PASSWORD` is getting its value? And what other values can we get?


Now, in the `mysql` cli, you can try running few commands to see what do we already have in the database:

```sql
mysql> use inventory;
mysql> show tables;
mysql> SELECT * FROM orders;
```

## TODO: 

- Find how many rows each table has.
- Display all the columns of each table.


### (Optional) How do we have the data in database?

- Checkout the github repo of the docker image which we pulled [debezium/docker-images](https://github.com/debezium/docker-images/tree/master/examples/mysql/1.0)

- For brainstorming session - Given [such](https://github.com/debezium/docker-images/blob/master/examples/mysql/1.0/Dockerfile) `Dockerfile` will you be able to setup your own docker image with the database?

- Take a thorough look in the configuration file of the mysql database in the git repo, 

-----------------------------------------------------
## Step 4 (Optional): Configure a new MySQL to use Debezium connector

Please note that so far we used a docker image that was provided by Debezium and we also looked at the configuration file. But if you are interested in setting up your own docker images then you can follow these instructions.

### MySQL docker image from docker-hub

- Pull docker image from docker hun `mysql:5.7`
- Start the container and follow these insturcutions once you go into the mysql cli


```bash
docker run -d -p 3306:3306 --name mysql1 mysql:5.7
docker exec -it mysql1 mysql -uroot -p
```
It will ask for the password, pass the password **mypassword**

Note - `The password promptor will not be visible`

### Create new user for Debezium

1. Creating new user
```sql
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
```

2. Grant permissions to that user
```sql
mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```

3. Finalize the permissions
```sql
mysql> FLUSH PRIVILEGES;
```

### Enabling Binary logging for MySQL replication | Debezium

1. Check if the log-bin option is already on or not.

```sql
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM information_schema.global_variables WHERE varia
```

If it's off, check the docker run command and a config file and add another volume mapping to it with these configurations

```ini
server-id         = 223344 
log_bin           = mysql-bin 
binlog_format     = ROW 
binlog_row_image  = FULL 
expire_logs_days  = 10 
```

Then, check the configuration again: 
```sql
mysql> SELECT variable_value as "BINARY LOGGING STATUS (log-bin) ::"
FROM information_schema.global_variables WHERE variable_name='log_bin';
```

### Enabling MySQL Global Transaction Identifiers (GTI) for Debezium

Global transaction identifiers (GTIDs) uniquely identify transactions that occur on a server within a cluster. Though not required for the Debezium MySQL connector, using GTIDs simplifies replication and allows you to more easily confirm if master and slave servers are consistent.

1. Enable gtid_mode
```sql
mysql> gtid_mode=ON
```

2. Enable enforce_gtid_consistency:
```sql
mysql> enforce_gtid_consistency=ON
```
3. Confirm the changes
```sql
mysql> show global variables like '%GTID%';
```

#### Setting up session timeouts & query log for Debezium

```sql
mysql> interactive_timeout=<duration-in-seconds>
mysql> wait_timeout= <duration-in-seconds>

mysql> binlog_rows_query_log_events=ON
```

--------------------------------------------------------

## Step 5: Kafka Connect

### Start Kafka Connect in a container

It is recommened to run kafka connect on containerized environment

Assuming you didn't face any issues so far, now it's time to run the command mentioned below to start the kafka-container

```bash
docker run -d -it \
--rm --name connect \
-p 8083:8083 \
-e GROUP_ID=1 \
-e CONFIG_STORAGE_TOPIC=my_connect_configs \
-e OFFSET_STORAGE_TOPIC=my_connect_offsets \
-e STATUS_STORAGE_TOPIC=my_connect_statuses \
--link zookeeper:zookeeper \
--link kafka:kafka \
--link mysql:mysql debezium/connect:1.0
```

Here are the things happening in the above command

- `link`: Connecting **zookeeper, kafka and mysql** container that we setup in the previous steps
- `p`: We are exposing port 8083:8083 to outside
- `e`: GROUP_ID, CONFIG_STORAGE_TOPIC, OFFSET_STORAGE_TOPIC, and STATUS_STORAGE_TOPIC - Setting these environment variables

You can remove `-d` from the above command to see what is going on in the container or the other option is to check the logs using `docker logs -f <container name>`


### Kafka Connect REST API 

We have exposed the kafka-connect service to our host machine at 8083 port. So, to communicate to the service, we can send requests to port 8083 oh our host machine which will forward our request to 8083 port of the container service. So, let's try that:

From terminal, request a curl command:
```bash
curl -H "Accept:application/json" localhost:8083/
```

If you are on Non-linux distro, try:
```bash
curl -H "Accept:application/json" <your ip>:8083/
```

You should receive a JSON response with `version` of Kafka connect and `commit` values.


Now, let's see the list of connectors available:

```bash
curl -H "Accept:application/json" localhost:8083/connectors/
```

It should return an empty list `[]`. In this case, we are good to go as we are able to talk to the connector.

So, let's start a connector that will capture the changes from our MSQL database.

### Monitoring MySQL Database

Since, we have `inventory` database which we have already talked about why? So, let's register a connector that will begin the monitoring with MySQL database server's binlog and generate change events for each row that are dynamically changing (if we do that). 
Since this is a new connector, when it starts it will start reading from the beginning of the MySQL binlog, which records all of the transactions, including individual row changes and changes to the schemas.

So for registring a connector, we will run the following curl command: (**PlEASE LOOK CAREFULLY, SOME FEILDS ARE LEFT BLANK, YOU HAVE TO FILL THEM**)

```bash
$ curl -i -X POST -H "Accept:application/json" -H \
"Content-Type:application/json" \
localhost:8083/connectors/ \
-d '{ "name": "inventory-connector", \
"config": { \
    "connector.class": "io.debezium.connectorsctor.mysql.MySqlConnector", \
    "tasks.max": "1", \
    "database.hostname": "mysql", \
    "database.port": "3306", \
    "database.user": "______", \
    "database.password": "______",\
    "database.server.id": "184054", \
    "database.server.name": "dbserver1", \
    "database.whitelist": "inventory", \
    "database.history.kafka.bootstrap.servers": "kafka:9092", \
    "database.history.kafka.topic": "dbhistory.inventory" \
} }'
```

So, we did the POST Request against /connectors. Now, you can try the previous curl request to check the list of connectors again.


If you still have the kafka-connector service's log open in the terminal (if not, then you can check the logs) then you can observe, how each of the row of data is getting fetched by the connector.

#### Observations and Discussions So Far

In the change events, you will observe the binlogs and the topics to which it is storing the information is :



Each topic name starts with **dbserver1** which we gave to the connector. 

- dbserver1
- dbserver1.inventory.products
- dbserver1.inventory.products_on_hand
- dbserver1.inventory.customers
- dbserver1.inventory.orders


The first is our schema change topic to which all of the DDL statements are written. The remaining four topics are used to capture the change events for each of our four tables, and their topic names include the database name (e.g., inventory) and the table name.


## Step 6: Observe who changes are captured

So, here are the things we need

- Keep the logs of the kafkaconnector running in a terminal using:
```bash
docker logs -f <kafka connector container name>
```

- Restart a new kafka container which is attached with a kafka topic
```bash
$ docker run -it \
--name watcher \
--rm --link zookeeper:zookeeper \
--link kafka:kafka debezium/kafka:1.0 \
watch-topic -a -k dbserver1.inventory.customers
```
So, what are the things happening here:
- `a`: Telling the watch-topic to show us all events since the beginning 
- `k`: Log should output the event's key.

TODO: Observe the changes in the log after removing `a` or `k` from the above bask command.


Now that we are observing the events while you make any update in the `customers` table, that we leave up to you.

**QUESTION:** What are the ALTER/UPDATE/DELETE commands you tried and how was the changes getting logged by the containers.

## Congratulations! You completed this lab.