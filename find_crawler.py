import os
import re

def find_crawler_usage():
    """Find which crawler is being used in the project"""
    
    print("🔍 Searching for crawler usage...\n")
    
    crawler_files = {
        'simple_crawler': False,
        'multi_engine_crawler': False,
        'omni_orchestrator': False
    }
    
    # Search all Python files
    for root, dirs, files in os.walk('.'):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                        # Check for imports
                        if 'simple_crawler' in content:
                            crawler_files['simple_crawler'] = True
                            print(f"📁 {filepath} imports/uses simple_crawler")
                        
                        if 'multi_engine_crawler' in content:
                            crawler_files['multi_engine_crawler'] = True
                            print(f"📁 {filepath} imports/uses multi_engine_crawler")
                            
                        if 'omni_orchestrator' in content:
                            crawler_files['omni_orchestrator'] = True
                            print(f"📁 {filepath} imports/uses omni_orchestrator")
                        
                        # Check for instantiation
                        if 'SimpleCrawler' in content:
                            print(f"🚀 {filepath} instantiates SimpleCrawler")
                            
                        if 'MultiEngineCrawler' in content:
                            print(f"🚀 {filepath} instantiates MultiEngineCrawler")
                            
                        if 'OmniOrchestrator' in content:
                            print(f"🚀 {filepath} instantiates OmniOrchestrator")
                            
                except Exception as e:
                    print(f"❌ Error reading {filepath}: {e}")
    
    print(f"\n📊 Usage Summary:")
    for crawler, used in crawler_files.items():
        status = "✅ USED" if used else "❌ NOT USED"
        print(f"   {crawler}: {status}")

if __name__ == "__main__":
    find_crawler_usage()