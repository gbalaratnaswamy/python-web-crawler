import pandas as pd
import sys
from secrets.mongodbfiles import getconnection  # connect to mongodb get secret sting
print(getconnection())