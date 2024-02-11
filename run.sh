#!/usr/bin/bash
targetjar="dummy-spark_2.12-0.1.0.jar"
docker run --rm \
    -v $(pwd)/passengers.csv:/home/passengers.csv \
	-v $(pwd)/target/scala-2.12/${targetjar}:/home/${targetjar} \
    myspark \
    /opt/spark/bin/spark-submit \
    --class jobs.Foo \
	--master local[1] \
	/home/${targetjar} \
	/home/passengers.csv
