import sys
print(sys.executable)
import db.models as models
print(models.__file__)
print(models.init_db)