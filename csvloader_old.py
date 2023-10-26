import csv
import redis

# Function to load CSV data into Redis with automatic key and field column detection
def load_csv_data_to_redis(csv_filename, redis_host, redis_port, redis_db, redis_password):
    r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    with open(csv_filename, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        header = csvreader.fieldnames
        key_column = header[0]
        field_columns = header[1:]

        for row in csvreader:
            key = row[key_column]
            field_data = {field: row[field] for field in field_columns}
            r.hset(key, mapping=field_data)

if __name__ == '__main__':
    # Define your CSV file, Redis connection details
    csv_filename = 'worldcities.csv'
    redis_host = 'localhost'
    redis_port = 6379
    redis_db = 0
    redis_password = None  # Set to your Redis password if required

    load_csv_data_to_redis(csv_filename, redis_host, redis_port, redis_db, redis_password)
