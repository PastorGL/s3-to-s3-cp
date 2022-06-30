# s3-to-s3-cp
As the name says, this utility copies everything from one S3 location (set by full path prefix) to another (again, puts it all under full path prefix).

# Build
with Maven:
```bash
mvn package
```

# Run
```bash
java -jar s3-to-s3-cp.jar
```
It'll print help message about its parameters, which are pretty self-explanatory. If you don't supply AWS keys at command line, it'll try to load them from your AWS CLI config.
