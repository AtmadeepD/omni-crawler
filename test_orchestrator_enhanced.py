import asyncio
import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from orchestrator.omni_orchestrator import OmniOrchestrator

async def test_orchestrator_enhanced():
    """Test the orchestrator with enhanced storage"""
    print("🧪 Testing Orchestrator with Enhanced Storage...")
    
    try:
        orchestrator = OmniOrchestrator()
        
        print("✅ Orchestrator initialized with enhanced storage")
        print(f"📊 Storage type: {type(orchestrator.storage).__name__}")
        
        # Test that enhanced methods are available
        has_enhanced_save = hasattr(orchestrator.storage, 'save_enhanced_article')
        has_enhanced_get = hasattr(orchestrator.storage, 'get_enhanced_articles')
        
        print(f"🔧 Enhanced save available: {has_enhanced_save}")
        print(f"🔧 Enhanced retrieval available: {has_enhanced_get}")
        
        if has_enhanced_save and has_enhanced_get:
            print("🎉 Orchestrator is ready for enhanced article processing!")
            
            # Test enhanced retrieval
            articles = orchestrator.storage.get_enhanced_articles(limit=3)
            print(f"📚 Found {len(articles)} enhanced articles in database")
            
            return True
        else:
            print("❌ Enhanced methods not available in orchestrator")
            return False
            
    except Exception as e:
        print(f"❌ Orchestrator test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_orchestrator_enhanced())
    if success:
        print("\n🎊 ORCHESTRATOR ENHANCED INTEGRATION SUCCESSFUL!")
    else:
        print("\n💥 ORCHESTRATOR ENHANCED INTEGRATION FAILED!")