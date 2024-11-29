
## Start Applications

```bash
#### build docker image for Pyflink
docker build -t=building-pyflink-apps:1.17.1 .

#### create kafka and flink clusters and kafka-ui
docker-compose up -d

#### start kafka producer in one terminal
python -m venv venv
source venv/bin/activate
# upgrade pip (optional)
pip install pip --upgrade
# install required packages
pip install -r requirements-dev.txt
## start with --create flag to create topics before sending messages
python3 produce_transactions.py --file fraudTest.csv  --bootstrap-servers 10.8.1.96:29092 --create --interval 1

#### submit pyflink apps in another terminal
## flight importer
docker exec jobmanager /opt/flink/bin/flink run \
    --python /tmp/src/flink_job.py \
    --pyFiles file:///tmp/src/models.py \
    -d

## usage calculator
docker exec jobmanager /opt/flink/bin/flink run \
    --python /tmp/src/s20_manage_state.py \
    --pyFiles file:///tmp/src/models.py,file:///tmp/src/utils.py \
    -d
```

