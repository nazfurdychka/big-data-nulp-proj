# big-data-nulp-proj

## README IS NOT VALID, WILL BE UPDATED SOON (OR NOT...)

## Installation
_TBD_: add section about repo initialization and cloning

## Docker container launch
This is a step-by-step guide **for Windows users** how to set up 
and launch Docker container with Airflow and PySpark in it.
1.  First of all make sure you have Docker Desktop installed on your device. 
    If not - follow the instructions from the official Docker website: https://docs.docker.com/get-docker/.
    You can check if it is installed properly using next terminal command:
    
        docker --version
    
    Expected output:
    
        Docker version 24.0.6, build ed223bc
    
2.  Then you have to build an image with PySpark. Navigate to project directory
    and execute command:
    
        docker build . --tag extended_airflow:latest
    
    > Note: the `--tag` option is used to specify the custom image name and version 
    > which will be used in next steps. `extended_airflow` is name and `latest` is version. 
    > You can change them if needed.
    
    Expected output:
    
        ...  
        [+] Building 10.5s (11/11) FINISHED  
        ...  
        What's Next?  
        ...

3.  Before creating containers you have to create folders for Airflow DAGs and logs:
    
        mkdir dags logs


4. Next step is using built image for creating extended airflow containers. It can be done using next command:

        docker compose up --build

    > Note: if you have changed the name of docker image in previous step,
    > then before building and running the containters you have to change it in docker-compose file as well.
    > Open `docker-compose.yaml` and change the name on line 53.

    This operation may take some time (up to 10 minutes) because it builds and launches airflow and all needed services in seperate containters.
    Expected output:

        ...
        ✔ redis 7 layers [⣿⣿⣿⣿⣿⣿⣿]      0B/0B      Pulled                                                                                                                                13.8s 
        ✔ postgres 12 layers [⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿]      0B/0B      Pulled                                                                                                                       26.1s
        ...
        [+] Running 9/9
        ✔ Network big-data-nulp-proj_default                Created                                                                                                                       0.2s 
        ✔ Volume "big-data-nulp-proj_postgres-db-volume"    Created                                                                                                                       0.0s 
        ✔ Container big-data-nulp-proj-postgres-1           Created                                                                                                                       0.7s 
        ✔ Container big-data-nulp-proj-redis-1              Created                                                                                                                       0.6s 
        ✔ Container big-data-nulp-proj-airflow-init-1       Created                                                                                                                       0.3s 
        ✔ Container big-data-nulp-proj-airflow-scheduler-1  Created                                                                                                                       0.6s 
        ✔ Container big-data-nulp-proj-airflow-webserver-1  Created                                                                                                                       0.6s 
        ✔ Container big-data-nulp-proj-airflow-triggerer-1  Created                                                                                                                       0.5s 
        ✔ Container big-data-nulp-proj-airflow-worker-1     Created                                                                                                                       0.6s 
        ...

    After creation of containters they will be automatically launched. Each airflow service outputs their own log with launch process.
    It is expected to see **warnings** in it but not the **errors**. When all services will be up, you will be able
    to see 6 active containers in Docker Desktop:
    ![image](https://github.com/nazfurdychka/big-data-nulp-proj/assets/63544400/ea0ba5ba-be64-418f-b15e-7009e20e33a0)
    
5.  Finally, we have to check if Airflow and PySpark were installed and launched successfully. Here is hot to do it:
    * Access Airflow UI at `localhost:8080` (type it in browser)
    * Login with default credentials `airflow` (username), `airflow` (password)
    * Open DAG `PYSPARK_TEST`
    * Unpause it by clicking toggle near its name
    * Trigger it by clicking ▷
    * Open `Graph` tab and wait for the end (task will have dark green flag with text `success`)
    * Click on the only task, open `Logs` and check if the version of PySpark was printed successfully
    * Done!

6.  Airflow DAGs are automatically pulled from `dags` folder because it is mounted to the containter, so there is no need for
    rebuilding the image to see the changes, only refreshing Airflow page is required. However, if you want to make changes
    into the environment where DAGs are running (adding new python libraries, configuring env variables, etc.) you will need to
    shut down the containters, rebuild the image and launch everything again (steps 2 and 3).
    
7.  To shut down the containters use this command:

        docker compose down --volumes --rmi all

    Expected output:

        [+] Running 12/12
        ✔ Container big-data-nulp-proj-airflow-webserver-1  Removed                                                                                                                      10.6s 
        ✔ Container big-data-nulp-proj-airflow-scheduler-1  Removed                                                                                                                       6.6s 
        ✔ Container big-data-nulp-proj-airflow-worker-1     Removed                                                                                                                       7.5s 
        ✔ Container big-data-nulp-proj-airflow-triggerer-1  Removed                                                                                                                       5.1s 
        ✔ Container big-data-nulp-proj-airflow-init-1       Removed                                                                                                                       0.0s 
        ✔ Container big-data-nulp-proj-redis-1              Removed                                                                                                                       0.9s 
        ✔ Container big-data-nulp-proj-postgres-1           Removed                                                                                                                       0.8s 
        ✔ Volume big-data-nulp-proj_postgres-db-volume      Removed                                                                                                                       0.1s 
        ✔ Image extended_airflow:latest                     Removed                                                                                                                       0.2s 
        ✔ Image postgres:13                                 Removed                                                                                                                       0.9s 
        ✔ Image redis:latest                                Removed                                                                                                                       0.2s 
        ✔ Network big-data-nulp-proj_default                Removed                                                                                                                       0.2s
    