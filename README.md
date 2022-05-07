# STEPS TO START:

- create virtual environment 
- download dependencies
- set path variable `AIRFLOW_HOME` to this directory
- set environment variables for twitter credentials and DB details
- run `airflow db init`
- you probably don't hav any airflow user yet, so create one, run `airflow users create` and follow instructions
- open current directory in two terminal windows run `airflow schuduler` in one and `airflow webserver` in other
- go to `localhost:8080` and login with the user credentials you just created
- you should be able to see `twitter_wh_dag` there