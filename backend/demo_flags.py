import os

# Global demo flag to gate external effects
DEMO_MODE = os.getenv("DEMO_MODE", "0") == "1" 