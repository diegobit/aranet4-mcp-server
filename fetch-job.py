import os
import asyncio
import yaml
from src.aranet import Aranet4Manager

if __name__ == "__main__":
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    aranet4_db = Aranet4Manager(
        device_name=config["device_name"],
        device_mac=config["device_mac"],
        db_path=os.path.expanduser(config["db_path"]),
        use_local_tz=config["use_local_tz"]
    )

    asyncio.run(aranet4_db.fetch_new_data(verbose=True))
