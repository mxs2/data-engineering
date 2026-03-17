from extract import Extract
from load import Load

extractor = Extract()
loader = Load()

ch = extractor.extract_country("China")
print(ch)
