/**
 * A C program to model the Slave that is controlled by a Master node.
 *
 * To properly compile this program see COMPILE:
 *
 * COMPILE: gcc slave.c -o slave
 *
 * To properly use this program see USAGE:
 *
 * USAGE: ./slave <MASTER_IP_ADDRESS>
 * e.g. ./slave "10.211.55.13"
 *
 * @author Nicholas Adamou
 * @author Jillian Shew
 * @author Bingzhen Li
 * @date 12/9/2019
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <pthread.h>

#include "lib/utilities.h"

typedef struct thread_attr thread_attr;
typedef struct Job Job;

struct thread_attr {
    char *master_address;
    int slave_id;

    bool terminated;
};

int connect_to_master(char *address);
void *send_cpu_utilization(void *argv);
void *listen_for_job_request(void * argv);

/**
 * Connects to the master node via a web socket connection.
 *
 * @param address The IPv4 address of the Master node.
 *
 * @return The id of the slave.
 */
int connect_to_master(char *address) {
    int master_socket, bytes, id;
    struct hostent *server_host;
    struct sockaddr_in master_address;

    /* Get master host from master name. */
    server_host = gethostbyname(address);

    /* Initialise IPv4 server address with master host. */
    memset(&master_address, 0, sizeof address);
    master_address.sin_family = AF_INET;
    master_address.sin_port = htons(LISTEN_FOR_SLAVES_PORT);
    memcpy(&master_address.sin_addr.s_addr, server_host->h_addr, server_host->h_length);

    /* Create TCP socket. */
    if ((master_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("[X] socket");
        exit(1);
    }

    /* Connect to socket with master's address. */
    if (connect(master_socket, (struct sockaddr *)&master_address, sizeof master_address) < 0) {
        perror("[X] connect");
        close(master_socket);
        exit(1);
    }

    printf("[+] Slave: has connected to the {LISTEN_FOR_SLAVES} socket on Master ('%s', %d).\n", inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

    char *slave_address = get_address();
    bytes = send(master_socket, slave_address, strlen(slave_address), 0);
    printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n", slave_address, inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

    char response[MAX_BUFFER_SIZE];
    bytes = recv(master_socket, response, sizeof(response), 0);
    printf("[Slave]: Received: [%s] from Master ('%s', %d).\n", response, inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

    char message[MAX_BUFFER_SIZE];
    sscanf(response, "%s %d", message, &id);

    close(master_socket);
    printf("[-] Slave: has disconnected from the {LISTEN_FOR_SLAVES} socket on Master ('%s', %d).\n", inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

    printf("\n");

    return strcmp(message, "{SUCCESSFULLY_ADDED_SLAVE}") != 0 ? id = -1 : id;
}

/**
 * Sends this hosts CPU Utilization value to the master node.
 *
 * @param argv The arguments passed to the send_cpu_utilization thread.
 */
void *send_cpu_utilization(void *argv) {
    thread_attr *attr = (thread_attr *)argv;

    int master_socket, bytes;
    struct hostent *server_host;
    struct sockaddr_in master_address;

    while(!attr->terminated) {
        /* Get master host from master name. */
        server_host = gethostbyname(attr->master_address);

        /* Initialise IPv4 server address with master host. */
        memset(&master_address, 0, sizeof attr->master_address);
        master_address.sin_family = AF_INET;
        master_address.sin_port = htons(SEND_CPU_UTILIZATION_PORT);
        memcpy(&master_address.sin_addr.s_addr, server_host->h_addr, server_host->h_length);

        /* Create TCP socket. */
        if ((master_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
            perror("[X] socket");

            continue;
        }

        /* Make sure current slave's address is a valid address */
        if (inet_pton(AF_INET, attr->master_address, &master_address.sin_addr) <= 0) {
            fputs("\nInvalid address / Address not supported.\n", stderr);

            close(master_socket);

            continue;
        }

        /* Connect to socket with master's address. */
        if (connect(master_socket, (struct sockaddr *)&master_address, sizeof master_address) < 0) {
            perror("[X] connect");
            close(master_socket);

            continue;
        }

        printf("[+] Slave: has connected to the {SEND_CPU_UTILIZATION} socket on Master ('%s', %d).\n", inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

        char payload[MAX_BUFFER_SIZE];
        snprintf(payload, sizeof(payload),"%d %f", attr->slave_id, calc_cpu_util());
        bytes = send(master_socket, payload, sizeof(payload), 0);
        printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n", payload, inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

        char response[MAX_BUFFER_SIZE];
        bytes = recv(master_socket, response, sizeof(response), 0);
        printf("[Slave]: Received: [%s] from Master ('%s', %d).\n", response, inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

        close(master_socket);
        printf("[-] Slave: has disconnected from the {SEND_CPU_UTILIZATION} socket on Master ('%s', %d).\n", inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

        printf("\n");

        sleep(rand() % MAX_SLEEP_TIME);
    }

    pthread_exit(NULL);
}

/**
 * Listens for job requests sent from the master node.
 *
 * @param argv The arguments passed to the listen_for_job_request thread.
 */
void *listen_for_job_request(void *argv) {
    thread_attr *attr = (thread_attr *)argv;

    int opt = 1;
    int slave_socket, master_socket, bytes;
    struct sockaddr_in slave_address, master_address;

    /* Create TCP socket. */
    if ((slave_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("[X] socket");
        exit(1);
    }

    /* Forcefully attaching socket to the desired port  */
    if (setsockopt(slave_socket, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        perror("[X] setsockopt");
        exit(1);
    }

    /* Initialise IPv4 address. */
    memset(&slave_address, 0, sizeof slave_address);
    slave_address.sin_family = AF_INET;
    slave_address.sin_port = htons(SEND_JOB_PORT);
    slave_address.sin_addr.s_addr = INADDR_ANY;

    /* Bind address to socket. */
    while (bind(slave_socket, (struct sockaddr *)&slave_address, sizeof slave_address) < 0) {
        perror("[X] bind");

        sleep(rand() % 5);
    }

    /* Listen for connections via the socket. */
    if (listen(slave_socket, 1) == -1) {
        perror("[X] listen");
        exit(1);
    }

    printf("[*] Slave is listening on ('%s', %d) for [{JOBS}].\n\n", "127.0.0.1", SEND_JOB_PORT);


    while(!attr->terminated) {
        /* Accept connection from master. */
        socklen_t master_address_len = sizeof master_address;
        master_socket = accept(slave_socket, (struct sockaddr *)&master_address, &master_address_len);

        if (master_socket == -1) {
            perror("[X] accept");
            continue;
        }

        printf("[+] Master ('%s', %d): has connected to {SEND_JOBS} socket on Slave.\n", inet_ntoa(master_address.sin_addr), ntohs(master_address.sin_port));

        char request[MAX_BUFFER_SIZE];
        bytes = recv(master_socket, request, sizeof(request), 0);
        printf("[Master]: Received Job Request: [%s] from Client ('%s', %d).\n", request, inet_ntoa(master_address.sin_addr), ntohs(master_address.sin_port));

        char *response;

        if (bytes > 0) {
            response = "{SUCCESSFULLY_RECEIVED_JOB_REQUEST}";
            bytes = send(slave_socket, response, strlen(response), 0);
            printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                   response,
                   inet_ntoa(master_address.sin_addr),
                   htons(master_address.sin_port)
            );

            char **data = split(request, ' ');

            Job *job = (Job *)malloc(sizeof(Job));
            job->input_file = createBuffer();
            job->executable = createBuffer();

            job->executable->file_name = data[0];
            job->executable->size = atoi(data[1]);
            job->input_file->file_name = data[2];
            job->input_file->size = atoi(data[3]);
            job->command = data[4];

            free(data);

            char executable[job->executable->size];
            char file_buffer[MAX_FILE_BUFFER_SIZE];

            for (int i = 0; i < job->executable->size; i += MAX_FILE_BUFFER_SIZE) {
                bytes = recv(master_socket, file_buffer, MAX_FILE_BUFFER_SIZE, 0);
                strcat(executable, file_buffer);
                printf("[Slave]: Received %d bytes for file %s.\n", i, job->executable->file_name);
            }

            if (job->executable->size % MAX_FILE_BUFFER_SIZE != 0) {
                bytes = recv(master_socket, file_buffer, job->executable->size % MAX_FILE_BUFFER_SIZE, 0);
                strcat(executable, file_buffer);
            }

            job->executable->data = executable;

            if (bytes > 0) {
                response = "{SUCCESSFULLY_RECEIVED_BUFFER}";
                bytes = send(slave_socket, response, strlen(response), 0);
                printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                       response,
                       inet_ntoa(master_address.sin_addr),
                       htons(master_address.sin_port)
                );

                char input_file[job->input_file->size];

                for (int i = 0; i < job->input_file->size; i += MAX_FILE_BUFFER_SIZE) {
                    bytes = recv(master_socket, file_buffer, MAX_FILE_BUFFER_SIZE, 0);
                    strcat(executable, file_buffer);
                    printf("[Slave]: Received %d bytes for file %s.\n", i, job->input_file->file_name);
                }

                if (job->input_file->size % MAX_FILE_BUFFER_SIZE != 0) {
                    bytes = recv(master_socket, file_buffer, job->input_file->size % MAX_FILE_BUFFER_SIZE, 0);
                    strcat(executable, file_buffer);
                }

                job->input_file->data = input_file;

                if (bytes > 0) {
                    response = "{SUCCESSFULLY_RECEIVED_BUFFER}";
                    bytes = send(slave_socket, response, strlen(response), 0);
                    printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                           response,
                           inet_ntoa(master_address.sin_addr),
                           htons(master_address.sin_port)
                    );

                    write_file(job->executable->file_name, job->executable, "wb");
                    write_file(job->input_file->file_name, job->input_file, "w");

                    if (does_file_exist(job->executable->file_name) && does_file_exist(job->input_file->file_name)) {
                        execute(job->command);

                        char command[MAX_BUFFER_SIZE];
                        snprintf(command, MAX_BUFFER_SIZE, "rm %s %s", job->executable->file_name, job->input_file->file_name);
                        execute(command);

                        char output_file_name[20];
                        snprintf(output_file_name, sizeof(output_file_name), "%s_output.txt", job->executable->file_name);

                        if (does_file_exist(output_file_name)) {
                            char buffer[MAX_FILE_BUFFER_SIZE];
                            Buffer *output = read_file(output_file_name, "r");

                            printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                                   output->file_name,
                                   inet_ntoa(master_address.sin_addr),
                                   htons(master_address.sin_port)
                            );

                            printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                                   output->file_name,
                                   inet_ntoa(master_address.sin_addr),
                                   htons(master_address.sin_port)
                            );

                            FILE *stream = fmemopen(output->data, output->size, "r");

                            for (int i = 0; i < job->executable->size; i += MAX_FILE_BUFFER_SIZE) {
                                fread(buffer, sizeof(char), MAX_FILE_BUFFER_SIZE, stream);
                                bytes = send(master_socket, buffer, MAX_FILE_BUFFER_SIZE, 0);
                            }

                            if (job->input_file->size % MAX_FILE_BUFFER_SIZE != 0) {
                                fread(buffer, sizeof(char), job->input_file->size % MAX_FILE_BUFFER_SIZE, stream);
                                bytes = send(master_socket, buffer, job->input_file->size % MAX_FILE_BUFFER_SIZE, 0);
                            }

                            fclose(stream);

                            if (bytes > 0 && strcmp(response, "{SUCCESSFULLY_RECEIVED_BUFFER}\0") == 0) {
                                snprintf(command, sizeof(command), "rm %s", output_file_name);
                                execute(command);
                            } else {
                                response = "{FAILED_TO_RECEIVE_BUFFER}";

                                fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

                                bytes = send(slave_socket, response, strlen(response), 0);
                                printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                                       response,
                                       inet_ntoa(master_address.sin_addr),
                                       htons(master_address.sin_port)
                                );
                            }
                        }
                    }
                } else {
                    response = "{FAILED_TO_RECEIVE_BUFFER}";

                    fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

                    bytes = send(slave_socket, response, strlen(response), 0);
                    printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                           response,
                           inet_ntoa(master_address.sin_addr),
                           htons(master_address.sin_port)
                    );
                }
            } else {
                response = "{FAILED_TO_RECEIVE_BUFFER}";

                fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

                bytes = send(slave_socket, response, strlen(response), 0);
                printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                       response,
                       inet_ntoa(master_address.sin_addr),
                       htons(master_address.sin_port)
                );
            }
        } else {
            response = "{FAILED_TO_RECEIVE_JOB_REQUEST}";

            fputs("{FAILED_TO_RECEIVE_JOB_REQUEST}\n", stderr);

            bytes = send(slave_socket, response, strlen(response), 0);
            printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                   response,
                   inet_ntoa(master_address.sin_addr),
                   htons(master_address.sin_port)
            );
        }

        close(master_socket);
        printf("[-] Master ('%s', %d): has disconnected from {SEND_JOB} socket on Slave.\n", inet_ntoa(master_address.sin_addr), ntohs(master_address.sin_port));

        printf("\n");
    }

    pthread_exit(NULL);
}

int main(int argc, char **argv) {
    srand(time(0));

    char *address;

    /* Get Master's IP Address from command line arguments or stdin. */
    address = argc > 1 ? argv[1] : 0;
    if (!address) {
        printf("Enter Master's IP Address: ");
        scanf("%s", address);
    }

    int slave_id = connect_to_master(address);

    if (slave_id == -1) return -1;

    pthread_t send_cpu_utilization_thread;
    pthread_t listen_for_job_request_thread;

    thread_attr *attr = (thread_attr *)malloc(sizeof(thread_attr));

    if (!attr) {
        perror("[X] malloc");
        exit(1);
    }

    attr->master_address = address;
    attr->slave_id = slave_id;
    attr->terminated = false;

    pthread_create(&send_cpu_utilization_thread, NULL, send_cpu_utilization, (void *) attr);
    pthread_create(&listen_for_job_request_thread, NULL, listen_for_job_request, (void *) attr);

    while(!attr->terminated);

    attr->terminated = true;

    pthread_join(send_cpu_utilization_thread, NULL);
    pthread_join(listen_for_job_request_thread, NULL);

    free(attr);

    return 0;
}