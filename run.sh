#!/usr/bin/bash
docker run --rm \
    -v $(pwd)/passengers.csv:/home/passengers.csv \
	-v $(pwd)/target/scala-2.12/dummy-spark_2.12-0.1.0.jar:/home/dummy-spark_2.12-0.1.0.jar \
    myspark \
    /opt/spark/bin/spark-submit \
    --class MySparkApp \
	--master local[1] \
	/home/dummy-spark_2.12-0.1.0.jar \
	/home/passengers.csv
