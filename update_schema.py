import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from storage.database_schema_updater import DatabaseSchemaUpdater
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Run schema update standalone"""
    print("🔄 Updating database schema...")
    
    updater = DatabaseSchemaUpdater()
    success = updater.update_schema()
    
    if success:
        print("🎉 Database schema updated successfully!")
    else:
        print("❌ Database schema update failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()