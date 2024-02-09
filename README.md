# How to test
Use `sbt test`. To run integration tests you may need extra params.

# How to run
## A. (PREFERRED) Submit spark application

### Install dockerized `spark-submit` 
Pull `spark` from either [this](https://hub.docker.com/r/apache/spark) or [this](https://hub.docker.com/_/spark).

---
Take inspiration from the following example to submit an application that processes a local CSV:
```bash
docker run --rm \
	-v <path-to-your-JAR>:<desired-JAR-path-in-docker> \
    -v <path-to-your-CSV>:<desired-CSV-path-in-docker> \
    <SPARK_IMAGE>:<TAG> \
    /opt/spark/bin/spark-submit \
    --class <YourClass> \
	--master local[1] \
    <desired-JAR-path-in-docker> \
    <desired-CSV-path-in-docker>
```
You can also use the example `run.sh`

## B. Use `sbt run`
Just pass the local file path.
WARNING: You may run into issues due to lacking SparkSession build options (i.e. `"master=local[*]"`)
