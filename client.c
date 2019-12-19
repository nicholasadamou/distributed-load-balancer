/**
 * A C program used to simulate a Client node, which sends jobs to the master node.
 *
 * To properly compile this program see COMPILE:
 *
 * COMPILE: gcc client.c -o client
 *
 * To properly use this program see USAGE:
 *
 * USAGE: ./client <MASTER_IP_ADDRESS>
 * e.g. ./client "10.211.55.13"
 *
 * @author Nicholas Adamou
 * @author Jillian Shew
 * @author Bingzhen Li
 * @date 12/13/2019
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <libgen.h>

#include "lib/utilities.h"

void send_job_to_master(int master_socket, struct sockaddr_in *master_address);
void connect_to_master(char *address);

/**
 * Sends a job to the master node.
 *
 * @param master_socket The master socket for accepting client requests.
 * @param master_address The master host address.
 */
void send_job_to_master(int master_socket, struct sockaddr_in *master_address) {
    int bytes;
    char *payload = NULL;
    char *request = NULL;
    FILE *file;

    request = get_user_input("[?] Enter a job > ");
    char **data = split(request, ' ');

    Buffer *b1 = read_file(data[0], "rb");
    Buffer *b2 = read_file(data[1], "r");

    char job_request[MAX_BUFFER_SIZE];
    sprintf(job_request, "%s %d %s %d", basename(b1->file_name), b1->size, basename(b2->file_name), b2->size);

    bytes = send(master_socket, job_request, sizeof(job_request), 0);
    printf("[Client]: Sending Job Request: [%s] to Master ('%s', %d).\n",
           job_request,
           inet_ntoa((*master_address).sin_addr),
           htons((*master_address).sin_port)
    );

    char response[MAX_BUFFER_SIZE];
    bytes = recv(master_socket, response, sizeof(response), 0);
    printf("[Client]: Received: [%s] from Master ('%s', %d).\n",
           response,
           inet_ntoa((*master_address).sin_addr),
           ntohs((*master_address).sin_port)
    );

    if (bytes > 0 && strcmp(response, "{SUCCESSFULLY_RECEIVED_JOB_REQUEST}\0") == 0) {
        payload = (char *)malloc(sizeof(char) * MAX_FILE_BUFFER_SIZE);

        file = fopen(data[0], "rb");

        if (!file) {
            perror("[X] fopen");
            exit(1);
        }

        printf("[Client]: Sending: [%s] to Master ('%s', %d).\n",
               b1->file_name,
               inet_ntoa((*master_address).sin_addr),
               htons((*master_address).sin_port)
        );

        for (int i = 0; i < b1->size; i += MAX_FILE_BUFFER_SIZE) {
            fread(payload, sizeof(char), MAX_FILE_BUFFER_SIZE, file);
            send(master_socket, payload, MAX_FILE_BUFFER_SIZE, 0);
        }

        if (b1->size % MAX_FILE_BUFFER_SIZE != 0) {
            fread(payload, sizeof(char), b1->size % MAX_FILE_BUFFER_SIZE, file);
            send(master_socket, payload, b1->size % MAX_FILE_BUFFER_SIZE , 0);
        }

        fclose(file);

        free(payload);

        bytes = recv(master_socket, response, sizeof(response), 0);
        printf("[Client]: Received: [%s] from Master ('%s', %d).\n",
               response,
               inet_ntoa((*master_address).sin_addr),
               ntohs((*master_address).sin_port)
        );

        if (bytes > 0 && strcmp(response, "{SUCCESSFULLY_RECEIVED_BUFFER}\0") == 0) {
            payload = (char *)malloc(sizeof(char) * MAX_FILE_BUFFER_SIZE);

            file = fopen(data[1], "r");

            if (!file) {
                perror("[X] fopen");

                exit(1);
            }

            printf("[Client]: Sending: [%s] to Master ('%s', %d).\n",
                   b2->file_name,
                   inet_ntoa((*master_address).sin_addr),
                   htons((*master_address).sin_port)
            );

            for (int i = 0; i < b2->size; i += MAX_FILE_BUFFER_SIZE) {
                fread(payload, sizeof(char), MAX_FILE_BUFFER_SIZE, file);
                send(master_socket, payload, MAX_FILE_BUFFER_SIZE, 0);
            }

            if (b2->size % MAX_FILE_BUFFER_SIZE != 0) {
                fread(payload, sizeof(char), b2->size % MAX_FILE_BUFFER_SIZE, file);
                send(master_socket, payload, b2->size % MAX_FILE_BUFFER_SIZE , 0);
            }

            fclose(file);

            free(payload);

            bytes = recv(master_socket, response, sizeof(response), 0);
            printf("[Client]: Received: [%s] from Master ('%s', %d).\n",
                   response,
                   inet_ntoa((*master_address).sin_addr),
                   ntohs((*master_address).sin_port)
            );

            if (bytes > 0 && strcmp(response, "{SUCCESSFULLY_RECEIVED_BUFFER}\0") == 0) {
                bzero(response, sizeof(response));

                payload = "{REQUEST_JOB_OUTPUT}";
                bytes = send(master_socket, payload, strlen(payload), 0);
                printf("[Client]: Sending: [%s] to Master ('%s', %d).\n",
                       payload,
                       inet_ntoa((*master_address).sin_addr),
                       htons((*master_address).sin_port)
                );

                bytes = recv(master_socket, response, sizeof(response), 0);
                printf("[Client]: Received: [%s] from Master ('%s', %d).\n",
                       response,
                       inet_ntoa((*master_address).sin_addr),
                       ntohs((*master_address).sin_port)
                );

                if (bytes > 0) {
                    Buffer *output = createBuffer();
                    data = split(response, ' ');

                    output->file_name = data[0];
                    output->size = atoi(data[1]);

                    free(data);

                    payload = "{SUCCESSFULLY_RECEIVED_JOB_OUTPUT}";
                    bytes = send(master_socket, payload, strlen(payload), 0);
                    printf("[Client]: Sending: [%s] to Master ('%s', %d).\n",
                           payload,
                           inet_ntoa((*master_address).sin_addr),
                           htons((*master_address).sin_port)
                    );

                    char output_file[output->size];
                    char file_buffer[MAX_FILE_BUFFER_SIZE];

                    for (int i = 0; i < output->size; i += MAX_FILE_BUFFER_SIZE) {
                        bytes = recv(master_socket, file_buffer, MAX_FILE_BUFFER_SIZE, 0);
                        strcat(output_file, file_buffer);
                        printf("[Client]: Received %d bytes for file %s.\n", i, output->file_name);
                    }

                    if (output->size % MAX_FILE_BUFFER_SIZE != 0) {
                        bytes = recv(master_socket, file_buffer, output->size % MAX_FILE_BUFFER_SIZE, 0);
                        strcat(output_file, file_buffer);
                        printf("[Client]: Received %d bytes for file %s.\n", bytes, output->file_name);
                    }

                    output->data = output_file;

                    if (bytes > 0) {
                        payload = "{SUCCESSFULLY_RECEIVED_BUFFER}";
                        bytes = send(master_socket, payload, strlen(payload), 0);
                        printf("[Client]: Sending: [%s] to Master ('%s', %d).\n",
                               payload,
                               inet_ntoa((*master_address).sin_addr),
                               htons((*master_address).sin_port)
                        );

                        write_file(output->file_name, output, "w");

                        printf("[Client]: Job Output: [%d] from Master('%s', %d).\n",
                            atoi(output->data),
                            inet_ntoa((*master_address).sin_addr),
                            htons((*master_address).sin_port)
                        );

                        if (does_file_exist(output->file_name)) {
                            char command[MAX_BUFFER_SIZE];
                            snprintf(command, MAX_BUFFER_SIZE, "rm %s", output->file_name);
                            execute(command);
                        }
                    } else {
                        fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);
                    }
                } else {
                    fputs("{FAILED_TO_RECEIVE_JOB_OUTPUT}\n", stderr);
                }
            } else {
                fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);
            }
        } else {
            fputs("{FAILED_TO_RECEIVE_BUFFER}\n", stderr);
        }
    } else {
        fputs("{FAILED_TO_RECEIVE_JOB_REQUEST}\n", stderr);
    }

    free(data);
    free(request);
}

/**
 * Connects to the master node via a web socket connection.
 *
 * @param address The IPv4 address of the Master node.
 */
void connect_to_master(char *address) {
    int master_socket, bytes;
    struct hostent *server_host;
    struct sockaddr_in master_address;

    /* Get master host from master name. */
    server_host = gethostbyname(address);

    /* Initialise IPv4 address. */
    memset(&master_address, 0, sizeof address);
    master_address.sin_family = AF_INET;
    master_address.sin_port = htons(LISTEN_FOR_CLIENTS_PORT);
    memcpy(&master_address.sin_addr.s_addr, server_host->h_addr, server_host->h_length);

    /* Create TCP socket. */
    if ((master_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("[X] socket");
    }

    /* Connect to socket with master's address. */
    while (connect(master_socket, (struct sockaddr *)&master_address, sizeof master_address) == -1) {
        perror("[X] connect");

        sleep(rand() % 10);
    }

    printf("[+] Client: has connected to the {LISTEN_FOR_CLIENT} socket on Master ('%s', %d).\n", inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

    send_job_to_master(master_socket, &master_address);

    close(master_socket);
    printf("[-] Client: has disconnected from the {LISTEN_FOR_CLIENT} socket on Master ('%s', %d).\n", inet_ntoa(master_address.sin_addr), htons(master_address.sin_port));

    printf("\n");
}

int main(int argc, char **argv) {
    /* Get Master's IP address from command line arguments or stdin. */
    char *address = argc > 1 ? argv[1] : 0;
    if (!address) {
        printf("Enter Master's IP Address: ");
        scanf("%s", address);
    }

    connect_to_master(address);
}