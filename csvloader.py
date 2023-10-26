import csv
import redis

# Function to load data from CSV to Redis in the specified format
def load_csv_data_to_redis(csv_filename, redis_host, redis_port, redis_db, redis_password):
    r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

    with open(csv_filename, 'r', newline='') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            redis_key = f"ct:{row['id']}"
            # Create a Redis hash with the specified format
            redis_data = {
                "_id": row['id'],
                "name": row['city_ascii'],
                "country": row['country'],
                "population": row['population']
            }
            r.hmset(redis_key, redis_data)

if __name__ == '__main__':
    # Define your CSV file, Redis connection details
    csv_filename = 'worldcities.csv'  # Replace with your CSV file name
    redis_host = 'localhost'
    redis_port = 6379
    redis_db = 0
    redis_password = None  # Set to your Redis password if required

    load_csv_data_to_redis(csv_filename, redis_host, redis_port, redis_db, redis_password)
