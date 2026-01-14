from dotenv import load_dotenv
load_dotenv()

from buildops_master_sync.core.engine import MasterSyncEngine

if __name__ == "__main__":
    engine = MasterSyncEngine()
    engine.run()
    print("Master Sync run completed.")
