# cs764-Argo
Argo Json document store on Quickstep, Postgres, MongoDB [CS 764 Project Fall 2016]

Python Files:

driver.py - Main program that executes the experiment
mongo_driver.py - Program that executes the MongoDB NoBench queries Query 1 - Query 12
postgres_driver.py - Program that executes the PostgreSQL NoBench queries Query 1 - Query 12
quickstep_driver.py- Program that executes the Quickstep NoBench queries Query 1 - Query 12
Global.py - Configuration file for global variables [Eg. datasize]

Library
pyqs/pyqs.py for connecting to Quickstep via python

Execution
1. python driver.py - Generates data, loads data into all 3 systems, executes 10 runs of NoBench queries on all 3 systems
2. python driver.py --mongo --psql --qstep     - Same as 1
3. python driver.py --gen --load --run --mongo - Same as 1 but only on MongoDB
4. python driver.py --gen --load --run --mongo --noclean - Same as 1 but only on MongoDB and retains data in MongoDB after run
5. python driver.py --gen --load --run --test - Same as 1 but populates test data [smaller data size] in test tables on all 3 sys




