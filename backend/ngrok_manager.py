# from pyngrok import ngrok
# import time

# tunnel = ngrok.connect(
#     addr=3306,
#     proto="tcp"
# )

# print("MySQL tunnel:")
# print(tunnel.public_url)

# while True:
#     time.sleep(1000)

"""
Opens a TCP tunnel to local MySQL and keeps Databricks secrets in sync with it.

Free-tier ngrok hands out a new host:port on every restart, and that address is
needed in two places: the Databricks secret scope (Lane A JDBC) and the
Confluent connector config (Lane B Debezium). This syncs the first
automatically and prints the second for you to paste.
"""

import time
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient
from pyngrok import ngrok

SCOPE = "rl"
CHECK_EVERY_SECONDS = 30


def publish_to_databricks(host: str, port: str) -> None:
    client = WorkspaceClient()
    client.secrets.put_secret(scope=SCOPE, key="ngrok_host", string_value=host)
    client.secrets.put_secret(scope=SCOPE, key="ngrok_port", string_value=port)
    print(f"  databricks secrets updated: {SCOPE}/ngrok_host, {SCOPE}/ngrok_port")


def current_address():
    """Read the live tunnel address, reconnecting if the session dropped."""
    tunnels = ngrok.get_tunnels()
    if not tunnels:
        tunnels = [ngrok.connect(addr=3306, proto="tcp")]
    url = urlparse(tunnels[0].public_url)      # tcp://2.tcp.ngrok.io:11089
    return url.hostname, str(url.port)


def main():
    ngrok.connect(addr=3306, proto="tcp")
    last_seen = None

    while True:
        try:
            host, port = current_address()
        except Exception as e:
            print(f"  tunnel unavailable: {e}")
            time.sleep(CHECK_EVERY_SECONDS)
            continue

        if (host, port) != last_seen:
            print(f"\nMySQL tunnel is now {host}:{port}")
            publish_to_databricks(host, port)
            print("  paste into the Confluent connector settings:")
            print(f"     Database hostname : {host}")
            print(f"     Database port     : {port}")
            last_seen = (host, port)

        time.sleep(CHECK_EVERY_SECONDS)


if __name__ == "__main__":
    main()
