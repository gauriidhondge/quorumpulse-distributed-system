import os
NODES = {'pune': {'host': os.environ.get('PUNE_HOST', 'localhost'), 'port': int(os.environ.get('PUNE_PORT', 8001))}, 'mumbai': {'host': os.environ.get('MUMBAI_HOST', 'localhost'), 'port': int(os.environ.get('MUMBAI_PORT', 8002))}, 'nashik': {'host': os.environ.get('NASHIK_HOST', 'localhost'), 'port': int(os.environ.get('NASHIK_PORT', 8003))}}
REQUEST_SERVICE_PORT = int(os.environ.get('REQUEST_SERVICE_PORT', 8000))
SIGNALING_SERVER_PORT = int(os.environ.get('SIGNALING_SERVER_PORT', 8090))
SERVERLESS_GATEWAY_PORT = int(os.environ.get('SERVERLESS_GATEWAY_PORT', 8095))
SERVERLESS_GATEWAY_HOST = os.environ.get('SERVERLESS_GATEWAY_HOST', 'localhost')
SERVERLESS_GATEWAY_URL = f'http://{SERVERLESS_GATEWAY_HOST}:{SERVERLESS_GATEWAY_PORT}'
DFS_REPLICATION_FACTOR = int(os.environ.get('DFS_REPLICATION_FACTOR', 2))
HDFS_BASE_DIR = os.environ.get('HDFS_BASE_DIR', '/blood-network/uploads')
HDFS_BIN = os.environ.get('HDFS_BIN', 'hdfs')
HDFS_CALL_TIMEOUT_SECONDS = int(os.environ.get('HDFS_CALL_TIMEOUT_SECONDS', 30))
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
HEARTBEAT_INTERVAL_SECONDS = 5
STALE_THRESHOLD_SECONDS = 15
