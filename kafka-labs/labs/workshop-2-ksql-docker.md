<link rel='stylesheet' href='../assets/css/main.css'/>

[<< back to main index](../README.md)

# Workshop-2: Kafka Use Case: Click Stream Analysis of data using KSQL (Docker cli)

### Overview
The problem statement is very clear, we will try to observe the click stream data using KSQL in command line

### Depends On
- Docker > 1.11 version
- Docker compose 
- If you are running Linux as host, then for Elasticsearch container, make sure you run the following command first:
```bash
sudo sysctl -w vm.max_map_count=262144
```

### Run time
- 2 hr

## Step 1: Downloading and running the configured dockers

```bash
git clone https://github.com/confluentinc/examples.git
cd examples
git checkout 5.3.2-post
cd clickstream
```

Run the docker-compose command to start the containers.
You can take a look at the `docker-compose.yml` file which will tell you what are the container we are going to start.

```bash
docker-compose up -d
```

Docker-compose is a automated way of running a lot of container as a whole. Read more about docker compose [here](https://docs.docker.com/compose/)

Check the status of containers:
```bash
docker-compose ps 
```
You should observe the state as `UP` for all the containers

## Step 2: Creating Clickstream Data

A data generator is already running, simulating the stream of clicks. You can sample this stream by using a console consumer such as `kafkacat`:

```bash
docker-compose exec kafkacat \
        kafkacat -b kafka:29092 -C -c 10 -K: \
        -f '\nKey  : %k\t\nValue: %s\n' \
        -t clickstream
```

If you get the message `Broker: Leader not available`, try again after a moment, as the demo is still starting up.

Try removing `-c 10` and find out why only few streams (10) where visible.

Try to list the data with:
- status codes - by replacing `clickstream` with `clickstream_codes`
- user data - by replacing `clickstream` with `clickstream_users`



## Step 3: Bring KSQL to load Streaming Data

Launch KSQL CLI:

```bash
docker-compose exec ksql-cli ksql http://ksql-server:8080
```

You should now be in 
```sql
ksql>
```

## Step 4: Repeat the process of labs 8.3 for creating streaming data of single table

## Step 5: Moving the data to Graphana

Verify the data before moving

```sql
ksql>LIST TABLES;
```

Verify teh streams:
```sql
ksql>LIST STREAMS;
```

TODO: 
- Verify the clickstream data 
- Observe events per miniute


### Setting up elastic cash

Exit KSQL CLI by preessing
	- `CTRL+D`


Setting up Elasticserach document mapping template

```bash
cd examples/clickstream/ksql/ksql-clickstream-demo/demo/
```
Now, that we are into the right directoy, run these

```bash
docker-compose exec elasticsearch bash -c '/scripts/elastic-dynamic-template.sh'
docker-compose exec kafka-connect bash -c '/scripts/ksql-tables-to-grafana.sh'
```

Note that the scripts is in the repo, that you git cloned.


Load the dashboard of Graphana:

```bash
docker-compose exec grafana bash -c '/scripts/clickstream-analysis-dashboard.sh'
```


Open your browser using the URL output from the previous step, with the default login id and password as `admin` and `admin` respectively

Observe the dashboard demonstrates a series of streaming functionality.

## Congratulations! You have completed the lab!