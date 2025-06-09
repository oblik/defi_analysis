#!/usr/bin/env python3
"""
Example script demonstrating how to set up environment variables for API keys.
This script shows how to use python-dotenv to load API keys from a .env file.

Usage:
1. Create a .env file in the project root with your API keys:
   DUNE_API_KEY=your_dune_api_key_here

2. Run this script to test if the environment variables are properly loaded:
   python setup_env.py
"""

import os
import sys
from dotenv import load_dotenv

def main():
    """Main function to demonstrate environment variable setup"""
    print("Testing environment variable setup...")
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Check if DUNE_API_KEY is set
    dune_api_key = os.environ.get("DUNE_API_KEY")
    if dune_api_key:
        # Mask the API key for security
        masked_key = dune_api_key[:4] + "*" * (len(dune_api_key) - 8) + dune_api_key[-4:]
        print(f"✅ DUNE_API_KEY is set: {masked_key}")
    else:
        print("❌ DUNE_API_KEY is not set")
        print("\nTo set up your Dune API key, you can:")
        print("1. Create a .env file in the project root with:")
        print("   DUNE_API_KEY=your_dune_api_key_here")
        print("\n2. Or set it directly in your environment:")
        if sys.platform.startswith('win'):
            print("   set DUNE_API_KEY=your_dune_api_key_here")
        else:
            print("   export DUNE_API_KEY=your_dune_api_key_here")
    
    print("\nEnvironment setup complete!")

if __name__ == "__main__":
    main()
