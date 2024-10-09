# JMeter Results Aggregator

This repository contains a Python script that aggregates and analyzes JMeter results from a CSV file. The `AggregatorForJMeter` class provides methods to calculate various performance metrics, including response times, error percentages, throughput, and bytes sent/received.

## Features

- **Sample Counting**: Counts the total number of samples per label.
- **Response Time Analysis**:
  - Average response time
  - Median response time
  - 90th, 95th, and 99th percentiles
  - Minimum and maximum response times
- **Error Rate Calculation**: Calculates the percentage of failed requests.
- **Throughput Calculation**: Computes the number of requests per second.
- **Data Size Analysis**: Computes the average sent and received bytes in kilobytes.
- **CSV Export**: Exports aggregated results to a CSV file.

## Prerequisites

- Python 3.x
- `pandas` library (will be installed automatically if not already available)

## Installation

1. Clone this repository or download the script.
2. Ensure you have Python installed on your machine.
3. If `pandas` is not installed, it will be automatically installed when you run the script.

## Usage

1. Place your JMeter CSV results file in an accessible location.
2. Update the path to the CSV file in the script:

   ```python
   data = pd.read_csv(r"C:\path\to\your\jmeter_results.csv")
