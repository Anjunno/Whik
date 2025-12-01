import os
import mysql.connector
from mysql.connector import pooling

db_pool = None

def get_connection():
    global db_pool
    if db_pool is None:
        print("DB 커넥션 풀을 새로 생성합니다...")
        pool_config = {
            "host": os.environ.get('DB_HOST'),
            "user": os.environ.get('DB_USER'),
            "password": os.environ.get('DB_PASSWORD'),
            "database": os.environ.get('DB_NAME'),
            "pool_name": "lambda_pool",
            "pool_size": 5,
        }
        db_pool = pooling.MySQLConnectionPool(**pool_config)
    
    print("풀에서 커넥션을 빌려옵니다.")
    return db_pool.get_connection()