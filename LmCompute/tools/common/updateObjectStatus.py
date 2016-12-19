"""
@summary: This script updates a Lifemapper object in the database
"""
import argparse

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="This script updates a Lifemapper object")
   # Inputs
   parser.add_argument("processType", help="The process type of the object to update")
   parser.add_argument("objectId", help="The id of the object to update")
   parser.add_argument("status", help="The new status of the object")
   
   args = parser.parse_args()
   
