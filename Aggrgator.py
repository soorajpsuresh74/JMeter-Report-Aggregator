try:
    import os
    from io import BytesIO
    import pandas as pd
    import boto3
    from dotenv import load_dotenv
    from botocore.exceptions import NoCredentialsError, ClientError

except ImportError:
    print("Error: Missing imports, Please install it by running:")
    print("pip install -r requirements.txt")
    exit(1)

load_dotenv()


class AggregatorForJMeter:
    def __init__(self, data):
        self.data = data
        self.data['timeStamp'] = pd.to_datetime(data['timeStamp'], unit='ms')
        self.grouped = self.data.groupby('label')
        self.state = True
        self.__debugger()

        if self.state:
            self.make_csv()

        self.s3_bucket_name = os.getenv('S3_BUCKET_NAME')
        self.file_name = 'Aggregated_report.csv'

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
        self.dump_to_s3(results)

    def dump_to_s3(self, results):
        s3 = boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        output_buffer = BytesIO()
        results.to_csv(output_buffer, index=False)
        output_buffer.seek(0)

        try:
            s3.upload_fileobj(output_buffer, self.s3_bucket_name, self.file_name)
            print(f"Successfully uploaded {self.file_name} to S3 bucket {self.s3_bucket_name}")
        except NoCredentialsError:
            print("Error: AWS credentials not found.")
        except ClientError as e:
            print(f"Client error: {e}")
        except Exception as e:
            print(f"Error uploading file to S3: {e}")


try:
    csv_file = pd.read_csv(input("Enter the file path: "))
    aggregator = AggregatorForJMeter(data=csv_file)
except RuntimeError:
    print("runtime_error")
