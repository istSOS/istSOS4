import os
from dotenv import load_dotenv

# Specify the path to the .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')

# Load the environment variables from the .env file
load_dotenv(dotenv_path)

# Access environment variables
pg_host = os.getenv('POSTGRES_HOST')
pg_port = os.getenv('POSTGRES_PORT')

print(f"postgres host: {pg_host}")
print(f"postgres port: {pg_port}")
