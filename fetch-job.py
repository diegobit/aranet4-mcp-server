import os
import asyncio
import yaml
from aranet import Aranet4DB

if __name__ == "__main__":
    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)

    aranet4_db = Aranet4DB(
        device_name=config["device_name"],
        device_mac=config["device_mac"],
        db_path=os.path.expanduser(config["db_path"]),
        use_local_tz=config["use_local_tz"]
    )

    asyncio.run(aranet4_db.fetch_new_data(verbose=True))
