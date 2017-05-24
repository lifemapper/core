"""
@summary: This script operates like 'touch' and creates a new file.  It will
             also create the directories if necessary
"""
import argparse

from LmBackend.common.lmobj import LmObject

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='This script touches a file')
   # Inputs
   parser.add_argument('file_name', type=str, help='The file path to touch')
   args = parser.parse_args()
   
   lmo = LmObject()
   lmo.readyFilename(args.file_name)
   
   with open(args.file_name, 'w') as outF:
      outF.write('')
   

