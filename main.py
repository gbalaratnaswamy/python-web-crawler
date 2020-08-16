import pandas as pd
import sys
try:
    command=sys.argv[1]
except IndexError:
    command=None 
if command!=None:

    print("you have entered",command)