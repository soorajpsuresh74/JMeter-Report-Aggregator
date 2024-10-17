import os
from io import BytesIO
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from constants import MyLogger, Secrets

logger = MyLogger()


class AggregatorForJMeter:
    def __init__(self, data):
        self.data = data
        self.data['timeStamp'] = pd.to_datetime(data['timeStamp'], unit='ms')
        self.grouped = self.data.groupby('label')
        # self.logger = MyLogger()

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

    def tester(self):
        logger.log_info(f"Sample Counter:\n, {self.sample_counter()}")
        logger.log_info(f"Average Response Time:\n, {self.average_response_time()}")
        logger.log_info(f"Median Response Time:\n, {self.median_response_time()}")
        logger.log_info(f"90th Percentile Response Time:\n, {self.percentile_90_response_times()}")
        logger.log_info(f"95th Percentile Response Time:\n, {self.percentile_95_response_times()}")
        logger.log_info(f"99th Percentile Response Time:\n, {self.percentile_99_response_times()}")
        logger.log_info(f"Min Response Time:\n, {self.min_response_time()}")
        logger.log_info(f"Max Response Time:\n, {self.max_response_time()}")
        logger.log_info(f"Error Percentage:\n, {self.error_percentage()}")
        logger.log_info(f"Throughput:\n, {self.calculate_throughput()}")
        logger.log_info(f"Sent Bytes (KB):\n, {self.calculate_sent_bytes_in_kb()}")
        logger.log_info(f"Received Bytes (KB):\n, {self.calculate_received_bytes_kb()}")

    def create_report(self):
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

        return results

    def save_report_locally(self):
        self.create_report().to_csv(Secrets.OUTPUT_FILE_NAME, index=False)
        full_path = os.path.abspath(Secrets.OUTPUT_FILE_NAME)
        logger.log_info(f"Success! File saved at: {full_path}")

    def save_to_s3(self):
        output_buffer = BytesIO()
        results = self.create_report()
        output_buffer.seek(0)
        s3 = boto3.client(
            's3',
            region_name=Secrets.AWS_REGION,
            aws_access_key_id=Secrets.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Secrets.AWS_SECRET_ACCESS_KEY
        )

        results.to_csv(index=False, encoding='utf-8')
        output_buffer.seek(0)

        try:
            s3.upload_fileobj(output_buffer, Secrets.S3_BUCKET_NAME, Secrets.OUTPUT_FILE_NAME)
            logger.log_info(f"Successfully uploaded {Secrets.OUTPUT_FILE_NAME} to S3 bucket {Secrets.S3_BUCKET_NAME}")
        except NoCredentialsError:
            print("Error: AWS credentials not found.")
        except ClientError as e:
            print(f"Client error: {e}")
        except Exception as e:
            print(f"Error uploading file to S3: {e}")


try:
    data = pd.read_csv(Secrets.FILE_PATH)
    aggregator = AggregatorForJMeter(data=data)

    if Secrets.SAVE_TO_S3:
        aggregator.save_to_s3()

    if Secrets.ENABLE_LOGGING:
        aggregator.tester()

    if Secrets.SAVE_REPORT_LOCALLY:
        aggregator.create_report()


except Exception as e:

    logger.log_error(f"Error occur during the creation : {e}")
