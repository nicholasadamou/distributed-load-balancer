/**
 * A C program used to simulate a Master node, which controls slaves.
 *
 * To properly compile this program see COMPILE:
 *
 * COMPILE: gcc master.c -lpthread -o master
 *
 * To properly use this program see USAGE:
 *
 * USAGE: ./master
 *
 * @author Nicholas Adamou
 * @author Jillian Shew
 * @author Bingzhen Li
 * @date 12/13/2019
 */

#include <stdio.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "lib/slavelist.h"
#include "lib/utilities.h"

typedef struct thread_attr thread_attr;
typedef struct Client Client;
typedef struct Job Job;

struct thread_attr {
    SlaveList *list;
    Slave *optimal_slave;
    bool terminated;
};

struct Client {
    int socket;
    struct sockaddr_in *address;

    thread_attr *attr;
};

void *listen_for_slaves(void *argv);
void *load_balance(void *argv);
Buffer *pass_job_to_optimal_slave(Job *job, Client *client);
void *handle_client(void *argv);
void *listen_for_clients(void *argv);

/**
 * Add a Slave to the network of Slave nodes.
 *
 * @param argv The arguments passed to the listen_for_slaves thread.
 */
void *listen_for_slaves(void *argv) {
    thread_attr *attr = (thread_attr *)argv;
    SlaveList *list = attr->list;

    int opt = 1;
    int master_socket, slave_socket, bytes;
    struct sockaddr_in slave_address, master_address;

    /* Create TCP socket. */
    if ((master_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("[X] socket");
        exit(1);
    }

    /* Forcefully attaching socket to the desired port  */
    if (setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        perror("[X] setsockopt");
        exit(1);
    }

    /* Initialise IPv4 address. */
    master_address.sin_family = AF_INET;
    master_address.sin_port = htons(LISTEN_FOR_SLAVES_PORT);
    master_address.sin_addr.s_addr = INADDR_ANY;

    /* Bind address to socket. */
    while (bind(master_socket, (struct sockaddr *)&master_address, sizeof master_address) < 0) {
        perror("[X] bind");

        sleep(rand() % 5);
    }

    /* Listen for connections via the socket. */
    if (listen(master_socket, MAX_BACKLOG) == -1) {
        perror("[X] listen");
        exit(1);
    }

    printf("[*] Master is listening on ('%s', %d) for Slaves.\n", "127.0.0.1", LISTEN_FOR_SLAVES_PORT);

    while(!attr->terminated) {
        /* Accept connection from slave. */
        socklen_t slave_address_len = sizeof slave_address;
        slave_socket = accept(master_socket, (struct sockaddr *)&slave_address, &slave_address_len);

        if (slave_socket == -1) {
            perror("[X] accept");
            continue;
        }

        printf("[+] Slave ('%s', %d): has connected to {LISTEN_FOR_SLAVES} socket.\n", inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

        char response[MAX_BUFFER_SIZE];
        bytes = recv(slave_socket, response, sizeof(response), 0);
        printf("[Master]: Received: [%s] from Slave ('%s', %d).\n", response, inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

        int id = add(list, response);
        printf("[Master] Added: [%s] to linked list of Slaves.\n", response);

        char payload[MAX_BUFFER_SIZE];
        char *key = response;

        if (searchList(list, key)) {
            snprintf(payload, sizeof(payload), "{SUCCESSFULLY_ADDED_SLAVE} %d", id);
        } else {
            snprintf(payload, sizeof(payload), "{FAILED_TO_ADD_SLAVE} %d", id);
        }

        bytes = send(slave_socket, payload, sizeof(payload), 0);
        printf("[Master]: Sending: [%s] to Slave ('%s', %d).\n", payload, inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

        close(slave_socket);
        printf("[-] Slave ('%s', %d): has disconnected from {LISTEN_FOR_SLAVES} socket.\n", inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

        printf("\n");
    }

    close(master_socket);

    pthread_exit(NULL);
}

/**
 * Listens for a client connection
 *
 * @param argv The arguments passed to the listen_for_clients thread.
 */
void *listen_for_clients(void *argv) {
    thread_attr *attr = (thread_attr *)argv;
    SlaveList *list = attr->list;

    int opt = 1;
    int master_socket, client_socket, bytes;
    struct sockaddr_in client_address, master_address;

    /* Create TCP socket. */
    if ((master_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("[X] socket");
        exit(1);
    }

    /* Forcefully attaching socket to the desired port  */
    if (setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        perror("[X] setsockopt");
        exit(1);
    }

    /* Initialise IPv4 address. */
    master_address.sin_family = AF_INET;
    master_address.sin_port = htons(LISTEN_FOR_CLIENTS_PORT);
    master_address.sin_addr.s_addr = INADDR_ANY;

    /* Bind address to socket. */
    while (bind(master_socket, (struct sockaddr *)&master_address, sizeof master_address) < 0) {
        perror("[X] bind");

        sleep(rand() % 5);
    }

    /* Listen for connections via the socket. */
    if (listen(master_socket, MAX_BACKLOG) == -1) {
        perror("[X] listen");
        exit(1);
    }

    printf("[*] Master is listening on ('%s', %d) for Clients.\n", "127.0.0.1", LISTEN_FOR_CLIENTS_PORT);

    while(!attr->terminated) {
        /* Accept connection from client. */
        socklen_t client_address_len = sizeof client_address;
        client_socket = accept(master_socket, (struct sockaddr *)&client_address, &client_address_len);

        if (client_socket == -1) {
            perror("[X] accept");
            continue;
        }

        Client *client = (Client *)malloc(sizeof(Client));

        client->socket = client_socket;
        client->address = &client_address;
        client->attr = attr;

        pthread_t handle_client_thread;

        pthread_create(&handle_client_thread, NULL, handle_client, (void *)client);

        printf("\n");
    }

    close(master_socket);

    pthread_exit(NULL);
}

/**
 * Handles / processes a given client connection.
 *
 * @param argv The arguments passed to the handle_client thread.
 */
void *handle_client(void *argv) {
    Client *client = (Client *)argv;

    Job *job = (Job *)malloc(sizeof(Job));
    job->input_file = createBuffer();
    job->executable = createBuffer();

    int bytes;
    char *response;
    char file_buffer[MAX_FILE_BUFFER_SIZE];

    printf("[+] Client ('%s', %d): has connected to {LISTEN_FOR_CLIENTS} socket.\n", inet_ntoa(client->address->sin_addr), ntohs(client->address->sin_port));

    char request[MAX_BUFFER_SIZE];
    bytes = recv(client->socket, request, sizeof(request), 0);
    printf("[Master]: Received Job Request: [%s] from Client ('%s', %d).\n", request, inet_ntoa(client->address->sin_addr), ntohs(client->address->sin_port));

    if (bytes > 0) {
        response = "{SUCCESSFULLY_RECEIVED_JOB_REQUEST}";
        bytes = send(client->socket, response, strlen(response), 0);
        printf("[Master]: Sending: [%s] to Client ('%s', %d).\n", response, inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

        char **data = split(request, ' ');

        job->executable->file_name = data[0];
        job->executable->size = atoi(data[1]);

        job->input_file->file_name = data[2];
        job->input_file->size = atoi(data[3]);

        sprintf(job->command, "./%s %s", job->executable->file_name, job->input_file->file_name);

        free(data);

        char executable_file[job->executable->size];

        for (int i = 0; i < job->executable->size; i += MAX_FILE_BUFFER_SIZE) {
            bytes = recv(client->socket, file_buffer, MAX_FILE_BUFFER_SIZE, 0);
            strcat(executable_file, file_buffer);
            printf("[Master]: Received %d bytes for file %s.\n", i, job->executable->file_name);
        }

        if (job->executable->size % MAX_FILE_BUFFER_SIZE != 0) {
            bytes = recv(client->socket, file_buffer, job->executable->size % MAX_FILE_BUFFER_SIZE, 0);
            strcat(executable_file, file_buffer);
        }

        job->executable->data = executable_file;

        if (bytes > 0) {
            response = "{SUCCESSFULLY_RECEIVED_BUFFER}";
            bytes = send(client->socket, response, strlen(response), 0);
            printf("[Master]: Sending: [%s] to Client ('%s', %d).\n", response, inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

            char input_file[job->input_file->size];

            for (int i = 0; i < job->input_file->size; i += MAX_FILE_BUFFER_SIZE) {
                bytes = recv(client->socket, file_buffer, MAX_FILE_BUFFER_SIZE, 0);
                strcat(input_file, file_buffer);
                printf("[Master]: Received %d bytes for file %s.\n", i, job->input_file->file_name);
            }

            if (job->input_file->size % MAX_FILE_BUFFER_SIZE != 0) {
                bytes = recv(client->socket, file_buffer, job->input_file->size % MAX_FILE_BUFFER_SIZE, 0);
                strcat(input_file, file_buffer);
            }

            job->input_file->data = input_file;

            if (bytes > 0) {
                response = "{SUCCESSFULLY_RECEIVED_BUFFER}";
                bytes = send(client->socket, response, strlen(response), 0);
                printf("[Master]: Sending: [%s] to Client ('%s', %d).\n", response, inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

                bytes = recv(client->socket, request, sizeof(request), 0);
                printf("[Master]: Received: [%s] from Client ('%s', %d).\n", request, inet_ntoa(client->address->sin_addr), ntohs(client->address->sin_port));

                if (bytes > 0 && strcmp(request, "{REQUEST_JOB_OUTPUT}\0") == 0) {
                    Buffer *output = pass_job_to_optimal_slave(job, client);

                    char output_request[MAX_BUFFER_SIZE];
                    sprintf(output_request, "%s %d", basename(output->file_name), output->size);

                    bytes = send(client->socket, output_request, sizeof(output_request), 0);
                    printf("[Master]: Sending Job Output Request: [%s] to Client ('%s', %d).\n",
                           output_request,
                           inet_ntoa(client->address->sin_addr),
                           htons(client->address->sin_port)
                    );

                    bytes = recv(client->socket, response, sizeof(response), 0);
                    printf("[Master]: Received: [%s] from Client ('%s', %d).\n",
                           response,
                           inet_ntoa(client->address->sin_addr),
                           ntohs(client->address->sin_port)
                    );

                    if (bytes > 0 && strcmp(response, "{SUCCESSFULLY_RECEIVED_JOB_OUTPUT}\0") == 0) {
                        char buffer[MAX_FILE_BUFFER_SIZE];
                        FILE *stream;

                        printf("[Master]: Sending: [%s] to Client ('%s', %d).\n",
                               output->file_name,
                               inet_ntoa(client->address->sin_addr),
                               htons(client->address->sin_port)
                        );

                        stream = fmemopen(output->data, output->size, "r");
                        for (int i = 0; i < output->size; i += MAX_FILE_BUFFER_SIZE) {
                            fread(buffer, sizeof(char), MAX_FILE_BUFFER_SIZE, stream);
                            bytes = send(client->socket, buffer, MAX_FILE_BUFFER_SIZE, 0);
                        }

                        if (output->size % MAX_FILE_BUFFER_SIZE != 0) {
                            fread(buffer, sizeof(char), output->size % MAX_FILE_BUFFER_SIZE, stream);
                            bytes = send(client->socket, buffer, output->size % MAX_FILE_BUFFER_SIZE, 0);
                        }

                        fclose(stream);

                        bytes = recv(client->socket, request, sizeof(request), 0);
                        printf("[Master]: Received: [%s] from Client ('%s', %d).\n", request,
                               inet_ntoa(client->address->sin_addr), ntohs(client->address->sin_port));

                        if (bytes > 0 && strcmp(response, "{SUCCESSFULLY_RECEIVED_BUFFER}\0") == 0) {
                            close(client->socket);
                            printf("[-] Client ('%s', %d): has disconnected from {LISTEN_FOR_CLIENTS} socket.\n", inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

                            printf("\n");

                            free(job);
                            free(client);

                            pthread_exit(NULL);
                        } else {
                            response = "{FAILED_TO_RECEIVE_BUFFER}";

                            fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

                            bytes = send(client->socket, response, strlen(response), 0);
                            printf("[Master]: Sending: [%s] to Client ('%s', %d).\n", response, inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

                            close(client->socket);
                            printf("[-] Client ('%s', %d): has disconnected from {LISTEN_FOR_CLIENTS} socket.\n", inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

                            printf("\n");

                            free(job);
                            free(client);

                            pthread_exit(NULL);
                        }
                    } else {
                        response = "{FAILED_TO_RECEIVE_BUFFER}";

                        fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

                        bytes = send(client->socket, response, strlen(response), 0);
                        printf("[Master]: Sending: [%s] to Client ('%s', %d).\n", response, inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

                        close(client->socket);
                        printf("[-] Client ('%s', %d): has disconnected from {LISTEN_FOR_CLIENTS} socket.\n", inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

                        printf("\n");

                        free(job);
                        free(client);

                        pthread_exit(NULL);
                    }
                } else {
                    response = "{FAILED_TO_RECEIVE_JOB_OUTPUT}";

                    fputs("{FAILED_TO_RECEIVE_JOB_OUTPUT}\n", stderr);

                    bytes = send(client->socket, response, strlen(response), 0);
                    printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
                           response,
                           inet_ntoa(client->address->sin_addr),
                           htons(client->address->sin_port)
                    );

                    close(client->socket);
                    printf("[-] Client ('%s', %d): has disconnected from {LISTEN_FOR_CLIENTS} socket.\n", inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

                    printf("\n");

                    free(job);
                    free(client);

                    pthread_exit(NULL);
                }
            } else {
                response = "{FAILED_TO_RECEIVE_BUFFER}";

                fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

                bytes = send(client->socket, response, strlen(response), 0);
                printf("[Master]: Sending: [%s] to Client ('%s', %d).\n", response, inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

                close(client->socket);
                printf("[-] Client ('%s', %d): has disconnected from {LISTEN_FOR_CLIENTS} socket.\n", inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

                printf("\n");

                free(job);
                free(client);

                pthread_exit(NULL);
            }
        } else {
            response = "{FAILED_TO_RECEIVE_BUFFER}";

            fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

            bytes = send(client->socket, response, strlen(response), 0);
            printf("[Master]: Sending: [%s] to Client ('%s', %d).\n", response, inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

            close(client->socket);
            printf("[-] Client ('%s', %d): has disconnected from {LISTEN_FOR_CLIENTS} socket.\n", inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

            printf("\n");

            free(job);
            free(client);

            pthread_exit(NULL);
        }
    } else {
        response = "{FAILED_TO_RECEIVE_JOB_REQUEST}";

        fputs("{FAILED_TO_RECEIVE_JOB_REQUEST}\n", stderr);

        bytes = send(client->socket, response, strlen(response), 0);
        printf("[Slave]: Sending: [%s] to Master ('%s', %d).\n",
               response,
               inet_ntoa(client->address->sin_addr),
               htons(client->address->sin_port)
        );

        close(client->socket);
        printf("[-] Client ('%s', %d): has disconnected from {LISTEN_FOR_CLIENTS} socket.\n", inet_ntoa(client->address->sin_addr), htons(client->address->sin_port));

        printf("\n");

        free(job);
        free(client);

        pthread_exit(NULL);
    }
}

/**
 * Passes a job to the optimal slave node and receives its output.
 *
 * WARNING: 'pass_job_to_optimal_slave' malloc()s memory to '*output' which must be freed by
 * the caller.
 *
 * @param job The job to pass to the optimal slave.
 *
 * @return The output of the job to send back to the source client.
 */
Buffer *pass_job_to_optimal_slave(Job *job, Client *client) {
    int slave_socket, bytes;
    struct sockaddr_in slave_address;

    /* Initialise IPv4 address. */
    slave_address.sin_family = AF_INET;
    slave_address.sin_port = htons(SEND_JOB_PORT);

    while (client->attr->list->size <= 0);

    while(!client->attr->terminated) {
        Slave *optimal_slave = client->attr->optimal_slave;

        /* Create TCP socket. */
        if ((slave_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
            perror("[X] socket");
            printf("\n");
            continue;
        }

        /* Make sure optimal slave's address is a valid address */
        if (inet_pton(AF_INET, optimal_slave->address, &slave_address.sin_addr) <= 0) {
            fputs("\nInvalid address / Address not supported.\n", stderr);
            close(slave_socket);
            continue;
        }

        /* Connect to socket with optimal slaves's address. */
        if (connect(slave_socket, (struct sockaddr *)&slave_address, sizeof slave_address) < 0) {
            perror("[X] connect");
            printf("\n");
            close(slave_socket);

            sleep(10);

            continue;
        }

        printf("[+] Master: has connected to the {SEND_JOB} socket on Slave ('%s', %d).\n", inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

        char buffer[MAX_FILE_BUFFER_SIZE];
        char request[MAX_BUFFER_SIZE];
        char response[MAX_BUFFER_SIZE];
        char *status;
        FILE *stream;

        char job_request[MAX_BUFFER_SIZE];
        sprintf(job_request, "%s %d %s %d %s", basename(job->executable->file_name), job->executable->size, basename(job->input_file->file_name), job->input_file->size, job->command);

        bytes = send(slave_socket, job_request, sizeof(job_request), 0);
        printf("[Master]: Sending Job Request: [%s] to Optimal Slave ('%s', %d).\n",
               job_request,
               inet_ntoa(slave_address.sin_addr),
               htons(slave_address.sin_port)
        );

        bytes = recv(slave_socket, response, sizeof(response), 0);
        printf("[Master]: Received: [%s] from Optimal Slave ('%s', %d).\n",
               response,
               inet_ntoa(slave_address.sin_addr),
               ntohs(slave_address.sin_port)
        );

        if (bytes > 0 && strcmp(response, "{SUCCESSFULLY_RECEIVED_JOB_REQUEST}\0") == 0) {
            printf("[Master]: Sending: [%s] to Optimal Slave ('%s', %d).\n",
                   job->executable->file_name,
                   inet_ntoa(slave_address.sin_addr),
                   htons(slave_address.sin_port)
            );

            stream = fmemopen(job->executable->data, job->executable->size, "rb");
            for (int i = 0; i < job->executable->size; i += MAX_FILE_BUFFER_SIZE) {
                fread(buffer, sizeof(char), MAX_FILE_BUFFER_SIZE, stream);
                bytes = send(slave_socket, buffer, MAX_FILE_BUFFER_SIZE, 0);
            }

            if (job->executable->size % MAX_FILE_BUFFER_SIZE != 0) {
                fread(buffer, sizeof(char), job->executable->size % MAX_FILE_BUFFER_SIZE, stream);
                bytes = send(slave_socket, buffer, job->executable->size % MAX_FILE_BUFFER_SIZE, 0);
            }

            fclose(stream);

            bytes = recv(slave_socket, request, sizeof(request), 0);
            printf("[Master]: Received: [%s] from Optimal Slave ('%s', %d).\n", request, inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

            if (bytes > 0 && strcmp(response, "{SUCCESSFULLY_RECEIVED_BUFFER}\0") == 0) {
                printf("[Master]: Sending: [%s] to Optimal Slave ('%s', %d).\n",
                       job->input_file->file_name,
                       inet_ntoa(slave_address.sin_addr),
                       htons(slave_address.sin_port)
                );

                stream = fmemopen(job->input_file->data, job->input_file->size, "r");
                for (int i = 0; i < job->executable->size; i += MAX_FILE_BUFFER_SIZE) {
                    fread(buffer, sizeof(char), MAX_FILE_BUFFER_SIZE, stream);
                    bytes = send(slave_socket, buffer, MAX_FILE_BUFFER_SIZE, 0);
                }

                if (job->input_file->size % MAX_FILE_BUFFER_SIZE != 0) {
                    fread(buffer, sizeof(char), job->input_file->size % MAX_FILE_BUFFER_SIZE, stream);
                    bytes = send(slave_socket, buffer, job->input_file->size % MAX_FILE_BUFFER_SIZE, 0);
                }

                fclose(stream);

                bytes = recv(slave_socket, request, sizeof(request), 0);
                printf("[Master]: Received: [%s] from Optimal Slave ('%s', %d).\n", request, inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

                if (bytes > 0 && strcmp(response, "{SUCCESSFULLY_RECEIVED_BUFFER}\0") == 0) {
                    bytes = recv(slave_socket, request, sizeof(request), 0);
                    printf("[Master]: Received Job Output: [%s] from Optimal Slave ('%s', %d).\n", request, inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

                    if (bytes > 0) {
                        status = "{SUCCESSFULLY_RECEIVED_JOB_OUTPUT}";
                        bytes = send(slave_socket, status, strlen(status), 0);
                        printf("[Master]: Sending: [%s] to Optimal Slave ('%s', %d).\n", status, inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

                        Buffer *output = createBuffer();

                        char **data = split(request, ' ');

                        output->file_name = data[0];
                        output->size = atoi(data[1]);

                        free(data);

                        char output_file[job->executable->size];

                        for (int i = 0; i < job->executable->size; i += MAX_FILE_BUFFER_SIZE) {
                            bytes = recv(slave_socket, buffer, MAX_FILE_BUFFER_SIZE, 0);
                            strcat(output_file, buffer);
                            printf("[Master]: Received %d bytes for file %s.\n", i, job->executable->file_name);
                        }

                        if (job->executable->size % MAX_FILE_BUFFER_SIZE != 0) {
                            bytes = recv(slave_socket, buffer, job->executable->size % MAX_FILE_BUFFER_SIZE, 0);
                            strcat(output_file, buffer);
                        }

                        if (bytes > 0) {
                            status = "{SUCCESSFULLY_RECEIVED_BUFFER}";
                            bytes = send(slave_socket, status, strlen(status), 0);
                            printf("[Master]: Sending: [%s] to Optimal Slave ('%s', %d).\n", status, inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

                            output->data = output_file;

                            return output;
                        } else {
                            status = "{FAILED_TO_RECEIVE_BUFFER}";

                            fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

                            bytes = send(slave_socket, status, strlen(status), 0);
                            printf("[Master]: Sending: [%s] to Optimal Slave ('%s', %d).\n", status, inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

                            close(slave_socket);
                            printf("[-] Master: has disconnected from {SEND_JOB} socket on Optimal Slave ('%s', %d).\n", inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

                            continue;
                        }
                    } else {
                        status = "{FAILED_TO_RECEIVE_JOB_OUTPUT}";

                        fputs("{FAILED_TO_RECEIVE_JOB_OUTPUT}\n", stderr);

                        bytes = send(slave_socket, status, strlen(status), 0);
                        printf("[Master]: Sending: [%s] to Optimal Slave ('%s', %d).\n", status, inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

                        close(slave_socket);
                        printf("[-] Master: has disconnected from {SEND_JOB} socket on Optimal Slave ('%s', %d).\n", inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

                        continue;
                    }
                } else {
                    status = "{FAILED_TO_RECEIVE_BUFFER}";

                    fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

                    bytes = send(slave_socket, status, strlen(status), 0);
                    printf("[Master]: Sending: [%s] to Optimal Slave ('%s', %d).\n", status, inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

                    close(slave_socket);
                    printf("[-] Master: has disconnected from {SEND_JOB} socket on Optimal Slave ('%s', %d).\n", inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

                    continue;
                }
            } else {
                status = "{FAILED_TO_RECEIVE_BUFFER}";

                fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);

                bytes = send(slave_socket, status, strlen(status), 0);
                printf("[Master]: Sending: [%s] to Optimal Slave ('%s', %d).\n", status, inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

                close(slave_socket);
                printf("[-] Master: has disconnected from {SEND_JOB} socket on Optimal Slave ('%s', %d).\n", inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

                continue;
            }
        } else {
            status = "{FAILED_TO_RECEIVE_JOB_REQUEST}";

            fputs("{FAILED_TO_RECEIVE_JOB_REQUEST}\n", stderr);

            bytes = send(slave_socket, status, strlen(status), 0);
            printf("[Master]: Sending: [%s] to Optimal Slave ('%s', %d).\n", status, inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

            close(slave_socket);
            printf("[-] Master: has disconnected from {SEND_JOB} socket on Optimal Slave ('%s', %d).\n", inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

            continue;
        }
    }
}

/**
 * Listens for CPU Utilization values of each slave in the cluster.
 *
 * Determines the "most optimal" slave based on its CPU Utilization.
 * If the CPU Utilization of the current slave is the smallest out of all the slaves in the cluster
 * then, it is the "most optimal" slave and has the highest priority of getting delegated a job.
 *
 * @param argv The arguments passed to the load_balance thread.
 */
void *load_balance(void *argv) {
    thread_attr *attr = (thread_attr *)argv;
    SlaveList *list = attr->list;

    int opt = 1;
    int master_socket, slave_socket, bytes;
    struct sockaddr_in slave_address, master_address;

    /* Create TCP socket. */
    if ((master_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("[X] socket");
        exit(1);
    }

    /* Forcefully attaching socket to the desired port  */
    if (setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        perror("[X] setsockopt");
        exit(1);
    }

    /* Initialise IPv4 address. */
    memset(&master_address, 0, sizeof master_address);
    master_address.sin_family = AF_INET;
    master_address.sin_port = htons(SEND_CPU_UTILIZATION_PORT);
    master_address.sin_addr.s_addr = INADDR_ANY;

    /* Bind address to socket. */
    while (bind(master_socket, (struct sockaddr *)&master_address, sizeof master_address) < 0) {
        perror("[X] bind");

        sleep(rand() % 5);
    }

    /* Listen for connections via the socket. */
    if (listen(master_socket, MAX_BACKLOG) == -1) {
        perror("[X] listen");
        exit(1);
    }

    while(list->size <= 0);

    printf("[*] Master is listening on ('%s', %d) for CPU Utilization.\n", "127.0.0.1", SEND_CPU_UTILIZATION_PORT);

    while(!attr->terminated) {
        /* Accept connection from slave. */
        socklen_t slave_address_len = sizeof slave_address;
        slave_socket = accept(master_socket, (struct sockaddr *)&slave_address, &slave_address_len);

        if (slave_socket == -1) {
            perror("[X] accept");
            continue;
        }

        printf("[+] Slave ('%s', %d): has connected to {LISTEN_FOR_CPU_UTILIZATION} socket.\n", inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

        char response[MAX_BUFFER_SIZE];
        bytes = recv(slave_socket, response, sizeof(response), 0);
        printf("[Master]: Received: [%s] from Slave ('%s', %d).\n", response, inet_ntoa(slave_address.sin_addr), ntohs(slave_address.sin_port));

        int slave_id;
        float slave_utilization;
        sscanf(response, "%d %f", &slave_id, &slave_utilization);

        char *payload;

        if (slave_id > list->size || slave_id < 0 || slave_utilization < 0) {
            payload = "{FAILED_TO_UPDATE_CPU_UTILIZATION}";
            bytes = send(slave_socket, payload, strlen(payload), 0);
            printf("[Master]: Sending: [%s] to Slave ('%s', %d).\n", payload, inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));
            close(slave_socket);

            continue;
        }

        payload = "{SUCCESSFULLY_UPDATED_CPU_UTILIZATION}";
        bytes = send(slave_socket, payload, strlen(payload), 0);
        printf("[Master]: Sending: [%s] to Slave ('%s', %d).\n", payload, inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

        Slave *current_slave = list->slaves[slave_id];
        current_slave->utilization = slave_utilization;

        if (attr->optimal_slave->utilization >= slave_utilization) {
            attr->optimal_slave = current_slave;
            printf("[Master]: Selected Slave ('%s', %d) as new optimal slave.\n", inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));
        }

        close(slave_socket);
        printf("[-] Slave ('%s', %d): has disconnected from the {LISTEN_FOR_CPU_UTILIZATION} socket.\n", inet_ntoa(slave_address.sin_addr), htons(slave_address.sin_port));

        printf("\n");
    }

    pthread_exit(NULL);
}

int main() {
    srand(time(0));

    SlaveList *slave_list = createSlaveList(MAX_BACKLOG);

    if (!slave_list) {
        perror("[X] malloc");
        exit(1);
    }

    pthread_t listen_for_clients_thread;
    pthread_t listen_for_slaves_thread;
    pthread_t load_balance_thread;

    thread_attr *argv = (thread_attr *)malloc(sizeof(thread_attr));

    if (!argv) {
        perror("[X] malloc");
        exit(1);
    }

    argv->list = slave_list;
    argv->optimal_slave = createSlave("0.0.0.0", -1);

    pthread_create(&listen_for_clients_thread, NULL, listen_for_clients, (void *) argv);
    pthread_create(&listen_for_slaves_thread, NULL, listen_for_slaves, (void *) argv);
    pthread_create(&load_balance_thread, NULL, load_balance, (void *) argv);

    while(!argv->terminated);

    argv->terminated = true;

    pthread_join(listen_for_clients_thread, NULL);
    pthread_join(listen_for_slaves_thread, NULL);
    pthread_join(load_balance_thread, NULL);

    cleanupList(argv->list);
    cleanupList(slave_list);
    free(argv);

    return 0;
}
