from dotenv import load_dotenv
import os
import subprocess

load_dotenv()

scope = os.getenv("SECRET_SCOPE")

secrets = {
    "mysql_user": os.getenv("MYSQL_USER"),
    "mysql_password": os.getenv("MYSQL_PASSWORD"),
    "ngrok_host": os.getenv("NGROK_HOST"),
    "ngrok_port": os.getenv("NGROK_PORT")
}


for key, value in secrets.items():
    cmd = [
        "databricks",
        "secrets",
        "put-secret",
        scope,
        key,
        "--string-value",
        value
    ]

    subprocess.run(cmd, check=True)

    print(f"Uploaded {key}")