# Assignment: Project 1
Northeaster University: CS6650: Building Scalable Distributed Systems</br>
Semester: Fall 2024 </br>

Student Name: Trung Ngo <br>
Student ID: 002847054


# Running instruction
## A. Configure remote local/remote server
1. Open the configuration file where in `src/main/resources/config.properties` is defined.
2. Uncomment the line for `localhost` if you're testing locally:
    ``` 
    basePath=http://localhost:8080/SkierServlet_war_exploded
    ```
3. For remote server configuration, comment out the `localhost` line and set the `basePath` to your EC2 instance's public IP:
    ```
    basePath=http://<your-ec2-ip>:8080/SkierServlet_war
    ```
## B. Build the swagger client 
- Follow instructions from the problem statement: https://github.khoury.northeastern.edu/vishalrajpal/cs6650/blob/main/assignments/Assignment-1.md#for-swagger-client

## B. Run the Client
1. Open `src/main/resources/HttpClientMultiThreaded.java`
2. Run the `main` function

