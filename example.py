from dune_client.client import DuneClient
import os

dune = DuneClient(os.environ.get("DUNE_API_KEY"))
query_result = dune.get_latest_result(4995589)