
######   Step to install and run apache airflow using docker in windows 11   ######
## Step 1: Prerequiresites ##
1. Install WS2 feature in windows
Install the following command from window command with administrator mode.
>> wsl --install
OR
>> wsl --update
2. Enable Virtualization in your system
Search - "Windows features on or off" and turn on following features 
  a. Virtual Machine
  b. Windows subsystem for Linux
  c. Hyper-V
Now open task mamanger page and navigate to Performance tab for making sure Virtualization status is Enabled over there.

## Step2: Install docker desktop for windows ##
Link - https://docs.docker.com/desktop/install/windows-install/

## Step3: Create a folder called APACHE-AIRFLOW-DOCKER and download docker-compse inside it using following command ##
>>  curl 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml' -o 'docker-compose.yaml' 

## Create sub folders using following commands  ##
mkdir ./dags ./logs  ./plugins

## Command to run docker composer ##
>> docker compose up airflow-init

## Command to start airflow services ##
>> docker compose up

## Command to check the status of airlfow services ##
>> docker ps

## Now you navigate to your browser and hit following url and log in.
URL: https://localhost:8080
Username: airflow
Password: airflow

## Command to stop all airflow services ## 
>> docker compoose down

<!-- Set up :: https://www.youtube.com/watch?v=Sva8rDtlWi4 -->
<!-- First Airflow :: https://www.youtube.com/watch?v=N3Tdmt1SRTM -->

<!-- To delete all containers including its volumes use, -->
>> docker rm -vf $(docker ps -aq)

<!-- To delete all the images, -->
>> docker rmi -f $(docker images -aq)


### Install extension ###
 <!-- 1. Dev containers -->

 ### Interact with docker container using command line interface ##
 >> docker exec <docker_container_id> airflow version

### Interact with airflow with api  ##
>> curl -X GET --user "airflow:airflow" "http:://localhost:8080/api/v1/dags"
## AIRFLOW__API__AUTH_BACKEND: 'airflow.api.auth.backend.basic_auth'


<!-- docker compose down --volumes --rmi all -->
