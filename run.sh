#!/usr/bin/bash
targetjar='dummy-spark_2.12-0.1.0.jar'
passengerDetailsFile='passengers.csv'
passengerFlightsFile='flightData.csv'
docker run --rm \
	-v $(pwd)/target/scala-2.12/${targetjar}:/home/${targetjar} \
    -v $(pwd)/${passengerFlightsFile}:/home/${passengerFlightsFile} \
    -v $(pwd)/${passengerDetailsFile}:/home/${passengerDetailsFile} \
    spark:3.5.0-scala2.12-java11-ubuntu \
    /opt/spark/bin/spark-submit \
    --class jobs.MostFrequentFliers \
	--master local[1] \
	/home/${targetjar} \
	/home/${passengerFlightsFile} \
	/home/${passengerDetailsFile}


# jobs
#    --class jobs.TotalFlightsPerMonth \
#    --class jobs.MostFrequentFliers \
