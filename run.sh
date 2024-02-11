#!/usr/bin/bash
usage() {
    echo "Usage: $0 <job_name>\n"
    echo "Available Job Names:"
    echo "    - TotalFlightsPerMonth"
    echo "    - MostFrequentFliers"
    echo "    - LongestRunOutsideUK"
    echo "    - TotalSharedFlights"
    echo "    - TotalSharedFlightsInRange"
    exit 1
}

# Check if at least one argument is provided
if [ $# -lt 1 ]; then
    usage
fi

# Validate the job name
case "$1" in
    TotalFlightsPerMonth|MostFrequentFliers|LongestRunOutsideUK|TotalSharedFlights|TotalSharedFlightsInRange)
    ;;
    *)
    echo "Error: Invalid job name '$1'"
    usage
    ;;
esac
#
# Other args that should be parametrized
fromDate='2017-01-01'
toDate='2017-02-01'
minFlights='3'

targetjar='dummy-spark_2.12-0.1.0.jar'
passengerDetailsFile='passengers.csv'
passengerFlightsFile='flightData.csv'

docker run --rm \
	-v $(pwd)/target/scala-2.12/${targetjar}:/app/${targetjar} \
    -v $(pwd)/${passengerFlightsFile}:/app/${passengerFlightsFile} \
    -v $(pwd)/${passengerDetailsFile}:/app/${passengerDetailsFile} \
    -v $(pwd)/output:/app/output \
    spark:3.5.0-scala2.12-java11-ubuntu \
    /opt/spark/bin/spark-submit \
	--master local[2] \
    --class jobs.${1} \
	/app/${targetjar} \
	/app/${passengerFlightsFile} \
	/app/${passengerDetailsFile} \
    ${fromDate} \
    ${toDate} \
    ${minFlights}
