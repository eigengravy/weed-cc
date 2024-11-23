- Run Flink Server `docker run \
    --rm \
    --name=jobmanager \
    --network flink-network \
    --publish 8081:8081 \
    --env FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
    flink:2.0-java11 jobmanager` and `docker run \
    --rm \
    --name=taskmanager \
    --network flink-network \
    --env FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
    flink:2.0-java11 taskmanager`
