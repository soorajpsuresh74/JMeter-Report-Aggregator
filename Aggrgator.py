import os
import pandas as pd


class AggregatorForJMeter:
    def __init__(self, data):
        self.data = data
        self.data['timeStamp'] = pd.to_datetime(data['timeStamp'], unit='ms')
        self.grouped = self.data.groupby('label')
        self.state = True
        self.__debugger()

        if self.state:
            self.make_csv()

    def sample_counter(self):
        return self.grouped.size()

    def average_response_time(self):
        return self.grouped['elapsed'].mean()

    def median_response_time(self):
        return self.grouped['elapsed'].median()

    def percentile_90_response_times(self):
        return self.grouped['elapsed'].quantile(0.9)

    def percentile_95_response_times(self):
        return self.grouped['elapsed'].quantile(0.95)

    def percentile_99_response_times(self):
        return self.grouped['elapsed'].quantile(0.99)

    def min_response_time(self):
        return self.grouped['Latency'].min()

    def max_response_time(self):
        return self.grouped['Latency'].max()

    def error_percentage(self):
        total_requests = self.grouped.size()
        self.data['is_failure'] = self.data['responseCode'] > 399
        failed_requests = self.data[self.data['is_failure']].groupby('label').size()
        error_percentage = (failed_requests / total_requests * 100).fillna(0)
        return error_percentage.astype(float)

    def calculate_throughput(self):
        def throughput_per_group(group):
            num_requests = len(group)
            if num_requests > 1:
                time_interval = group['timeStamp'].max() - group['timeStamp'].min()
                time_interval_seconds = time_interval.total_seconds()
                return num_requests / time_interval_seconds if time_interval_seconds > 0 else float('inf')
            else:
                return float('inf')

        return self.grouped.apply(throughput_per_group)

    def calculate_sent_bytes_in_kb(self):
        def calculate_size_of_sent_bytes(group):
            return group['sentBytes'].mean() * 1024

        sent_bytes = self.grouped.apply(calculate_size_of_sent_bytes)
        throughput = self.calculate_throughput()
        sent_bytes = sent_bytes * throughput / (1024 * 1024)

        return sent_bytes

    def calculate_received_bytes_kb(self):
        def calculate_size_of_response_bytes(group):
            return group['bytes'].mean() * 1024

        received_bytes = self.grouped.apply(calculate_size_of_response_bytes)
        throughput = self.calculate_throughput()
        received_bytes = received_bytes * throughput / (1024 * 1024)

        return received_bytes

    def __debugger(self):
        print("Sample Counter:\n", self.sample_counter())
        print("Average Response Time:\n", self.average_response_time())
        print("Median Response Time:\n", self.median_response_time())
        print("90th Percentile Response Time:\n", self.percentile_90_response_times())
        print("95th Percentile Response Time:\n", self.percentile_95_response_times())
        print("99th Percentile Response Time:\n", self.percentile_99_response_times())
        print("Min Response Time:\n", self.min_response_time())
        print("Max Response Time:\n", self.max_response_time())
        print("Error Percentage:\n", self.error_percentage())
        print("Throughput:\n", self.calculate_throughput())
        print("Sent Bytes (KB):\n", self.calculate_sent_bytes_in_kb())
        print("Received Bytes (KB):\n", self.calculate_received_bytes_kb())

    def make_csv(self):
        results = pd.DataFrame({
            '# Samples': self.sample_counter(),
            'Average': self.average_response_time(),
            'Median': self.median_response_time(),
            f'90% Line': self.percentile_90_response_times(),
            f'95% Line': self.percentile_95_response_times(),
            f'99% Line': self.percentile_99_response_times(),
            'Min': self.median_response_time(),
            'Max': self.max_response_time(),
            'Error %': self.error_percentage(),
            'Throughput': self.calculate_throughput(),
            'Received KB/sec': self.calculate_received_bytes_kb(),
            'Sent KB/sec': self.calculate_sent_bytes_in_kb()
        })
        results.round(2)

        file_name = 'Aggregated_report.csv'
        results.to_csv(file_name, index=False)
        full_path = os.path.abspath(file_name)
        print(f"Success! File saved at: {full_path}")


try:
    csv_file = pd.read_csv(input("Enter the file path: "))
    aggregator = AggregatorForJMeter(data=csv_file)
except RuntimeError:
    print("runtime_error")
