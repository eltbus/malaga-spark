run-TotalFlightsPerMonth:
	@bash run.sh TotalFlightsPerMonth

run-MostFrequentFliers:
	@bash run.sh MostFrequentFliers

run-LongestRunOutsideUK:
	@bash run.sh LongestRunOutsideUK

run-TotalSharedFlights:
	@bash run.sh TotalSharedFlights

run-TotalSharedFlightsInRange:
	@bash run.sh TotalSharedFlightsInRange

run-all:
	@bash run.sh TotalFlightsPerMonth
	@bash run.sh MostFrequentFliers
	@bash run.sh LongestRunOutsideUK
	@bash run.sh TotalSharedFlights
	@bash run.sh TotalSharedFlightsInRange
