# main.py

import asyncio
import importlib
import os
import sys
# Import init_db_collections and db_manager from utils.func
from utils.func import init_db_collections, db_manager 
from shared_client import start_client # Assuming this exists and handles your Telegram client logic

async def load_and_run_plugins():
    # Initialize the database collections before starting the client and plugins
    await init_db_collections() 
    
    # Start your Telegram client (assuming shared_client.py manages this)
    await start_client()
    
    plugin_dir = "plugins"
    plugins = [f[:-3] for f in os.listdir(plugin_dir) if f.endswith(".py") and f != "__init__.py"]

    for plugin in plugins:
        module = importlib.import_module(f"plugins.{plugin}")
        if hasattr(module, f"run_{plugin}_plugin"):
            print(f"Running {plugin} plugin...")
            await getattr(module, f"run_{plugin}_plugin")()  



async def main():
    await load_and_run_plugins()
    # Keep the main event loop running indefinitely
    while True:
        await asyncio.sleep(1)  

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    print("Starting application...")
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Shutting down application (KeyboardInterrupt)...")
    except Exception as e:
        print(f"An unexpected error occurred in main loop: {e}")
        sys.exit(1)
    finally:
        try:
            # Attempt to close the database connection pool gracefully
            if db_manager and db_manager._pool: 
                print("Closing database connections...")
                loop.run_until_complete(db_manager.close())
            
            # Close the asyncio loop
            if not loop.is_closed():
                loop.close()
                print("Asyncio loop closed.")
        except Exception as e:
            print(f"Error during final cleanup: {e}")