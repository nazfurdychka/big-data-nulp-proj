# big-data-nulp-proj

## Setting the project up and running
This is a step-by-step guide **for Windows users** how to set up 
and launch Docker container with Airflow and PySpark in it.
1. First of all make sure you have Docker Desktop installed on your device. 
    If not - follow the instructions from the official Docker website: https://docs.docker.com/get-docker/.
    You can check if it is installed properly using next terminal command:
    
        docker --version

    
2. Then you have to build an image with PySpark. Navigate firstly to project directory and then to 'docker' directory.
     After that execute command:
    
        docker build ./airflow --tag airflow_custom:latest
    
    > Note: the `--tag` option is used to specify the custom image name and version 
    > which will be used in next steps. `airflow_custom` is name and `latest` is version. 
    > You can change them if needed.
    
3. Next step is using built image for creating extended airflow containers. Run next command from same directory as previous step:

        docker compose up --build

    > Note: if you have changed the name of docker image in previous step,
    > then before building and running the containers you have to change it in docker-compose file as well.
    > Open `docker-compose.yaml` and change the name on line 53.

    This operation may take some time (up to 10 minutes) because it builds and launches airflow and all needed services in seperate containers.

    After creation of containers they will be automatically launched. Each airflow service outputs their own log with launch process.
    It is expected to see **warnings** in it but not the **errors**. When all services will be up, you will be able
    to see around 10 active Docker containers.
    > Note if as result appears error such as 'docker: Error response from daemon: failed to create shim task: OCI runtime create failed: runc create failed: unable to start container process: exec: "./entrypoint.sh": permission denied: unknown.' then add `RUN chmod +x entrypoint.sh` command in  [Spark Dockerfile](./docker/spark/Dockerfile) after `COPY entrypoint.sh .` 


4. Finally, we have to check if Airflow and PySpark were installed and launched successfully. Here is how to do it:
    * Access Airflow UI at `localhost:8080` (type it in browser)
    * Login with default credentials `airflow` (username), `airflow` (password)
    * Interact with Airflow and Spark Jobs  
   

5. Airflow DAGs are automatically pulled from `dags` folder because it is mounted to the container, so there is no need for
    rebuilding the image to see the changes, only refreshing Airflow page is required. However, if you want to make changes
    into the environment where DAGs are running (adding new python libraries, configuring env variables, etc.) you will need to
    shut down the containers, rebuild the image and launch everything again (steps 2 and 3).  

    
6. To shut down the containers use this command:

        docker compose down --volumes --rmi all
    