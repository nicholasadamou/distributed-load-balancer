#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <asm/errno.h>
#include <errno.h>
#include <sys/stat.h>
#include <limits.h>
#include <libgen.h>

#define MAX_BUFFER_SIZE 100
#define MAX_BACKLOG 100
#define MAX_SLEEP_TIME 10
#define MAX_FILE_BUFFER_SIZE 1000

#define LISTEN_FOR_SLAVES_PORT 8081
#define LISTEN_FOR_CLIENTS_PORT 8082
#define SEND_CPU_UTILIZATION_PORT 8083
#define SEND_JOB_PORT 8084

typedef struct Buffer Buffer;

struct Buffer {
    char *file_name;
    char *data;
    int size;
};

struct Job {
    Buffer *executable;
    Buffer *input_file;
    char *command;
};

char *get_address();
float calc_cpu_util();
char *execute(char *command);
Buffer *createBuffer();
Buffer *read_file(char *file_path, char *mode);
void write_file(char *file_path, Buffer *file, char *mode);
bool does_file_exist(char *file_path);
char* get_user_input(char *message);
char **split(char *str, const char separator);

/**
 * Obtains the IPv4 address of this given machine.
 *
 * @return the IPv4 address of this given machine.
 */
char *get_address() {
    return execute("hostname -I |  awk '{print $1}'");
}

/**
 * Calculates the CPU utilization of CPU0 on this given machine.
 *
 * @return the CPU utilization of CPU0 on this given machine.
 */
float calc_cpu_util() {
    FILE* fp = fopen("/proc/stat", "r");

    int i = 0;
    int sum = 0;
    int idle = 0;
    char str[MAX_BUFFER_SIZE];

    fgets(str, MAX_BUFFER_SIZE, fp);
    fclose(fp);

    char *token = strtok(str, " ");

    while(token != NULL) {
        token = strtok(NULL, " ");

        if(token != NULL) {
            sum += atoi(token);

            if(i == 3)
                idle = atoi(token);

            i++;
        }
    }


    return 1.0 - idle * 1.0 / sum;
}

/**
 * Execute a command line command on the system and get the response that is return.
 *
 * WARNING: 'execute' malloc()s memory to '*response' which must be freed by
 * the caller.
 *
 * @param path The path to the file to transfer.
 * @param target The host to transfer too.
 *
 * @return The response after executing the command line command.
 */
char *execute(char *command) {
    FILE *file = popen(command, "r");

    if (file) {
        char *response = (char *)malloc(sizeof(char) * MAX_BUFFER_SIZE);

        if (!response) {
            perror("[X] malloc");
            exit(1);
        }

        size_t n;

        while ((getline(&response, &n, file) > 0) && response) {
            // remove trailing new line character
            size_t ln = strlen(response) - 1;
            if (*response && response[ln] == '\n')
                response[ln] = '\0';
            pclose(file);
            return response;
        }
    }

    pclose(file);

    return NULL;
}

/**
 * Creates an empty Buffer structure.
 *
 * WARNING: 'createBuffer' malloc()s memory to '*buf' which must be freed by
 * the caller.
 *
 * @return An empty Buffer structure.
 */
Buffer *createBuffer() {
    Buffer *buf = (Buffer *)malloc(sizeof(Buffer));

    buf->file_name = NULL;
    buf->data = NULL;
    buf->size = -1;

    return buf;
}

/**
 * Reads the contents of a file into a character string.
 *
 * WARNING: 'read_file' malloc()s memory to '*data' which must be freed by
 * the caller.
 *
 * @param file_path The path to the binary executable.
 * @param mode The fopen() access mode.
 *
 * If reading a binary executable, the access mode must be "rb".
 * If reading a text file, the access mode must be "r".
 *
 * @return The file buffer structure representation of the file.
 */
Buffer *read_file(char *file_path, char *mode) {
    FILE *file = fopen(file_path, mode);

    if (!file) {
        fputs("[X] fopen: failed to open file.", stderr);
        exit(1);
    }

    char *data;
    long file_size;
    size_t result;

    fseek(file, 0, SEEK_END);
    file_size = ftell(file);
    rewind(file);

    data = (char *)malloc(sizeof(char) * file_size);

    if (!data) {
        perror("[X] malloc");
    }

    result = fread(data, 1, file_size, file);

    if (result != file_size) {
        perror("[X] fread");
    }

    fclose(file);

    Buffer *buf = (Buffer *)malloc(sizeof(Buffer));

    if (!buf) {
        perror("[X] malloc");
        exit(1);
    }

    buf->file_name = basename(file_path);
    buf->data = data;
    buf->size = result;

    return buf;
}

/**
 * Writes the contents of the data buffer into a file at the given path.
 *
 * @param file_path The path to save the data buffer to.
 * @param buf The file buffer structure created by read_file()
 * @param mode The fopen() access mode.
 *
 * If write to a binary executable, the access mode must be "wb".
 * If write to a text file, the access mode must be "w".
 */
void write_file(char *file_path, Buffer *buf, char *mode) {
    FILE *file = fopen(file_path, mode);

    if (!file) {
        perror("[X] fopen");
        exit(1);
    }

    fwrite(buf->data, sizeof(char), buf->size, file);

    fclose(file);

    if (strcmp(mode, "wb\0") == 0) {
        // make file executable
        char command[MAX_BUFFER_SIZE];
        sprintf(command, "chmod +x %s", basename(file_path));
        execute(command);
    }
}

/**
 * Determines if a given file located at the given path exists.
 *
 * @param file_path The path to the file.
 *
 * @return Whether or not the file exists.
 */
bool does_file_exist(char *file_path) {
    struct stat   buffer;
    return (stat (file_path, &buffer) == 0);
}

/**
 * Prompts the user to enter a string.
 *
 * WARNING: 'get_user_input' malloc()s memory to '*data' which must be freed by
 * the caller.
 *
 * @param message The message to prompt the user.
 *
 * @return The string read from the user.
 */
char* get_user_input(char *message) {
    char *data = (char *)malloc(sizeof(char) * MAX_BUFFER_SIZE);

    if (!data) {
        perror("[X] malloc");
        exit(1);
    }

    printf("%s", message);
    fgets(data, MAX_BUFFER_SIZE, stdin);
    fflush(stdin);

    // remove trailing new line character
    size_t ln = strlen(data) - 1;
    if (*data && data[ln] == '\n')
        data[ln] = '\0';

    return data;
}

/**
 * 'split' splits a string separated by a given char into a character array.
 *
 * WARNING: 'split' malloc()s memory to '**result' which must be freed by
 * the caller.
 *
 * @param str The string to split.
 * @param separator The separator character tto split by.
 *
 * @return The character array containing the each element split via the given separator.
 */
char **split(char *str, const char separator) {
    char **result = 0;
    size_t count = 0;
    char *tmp = str;
    char *last_comma = 0;
    char delim[2];
    delim[0] = separator;
    delim[1] = 0;

    // Count how many elements will be extracted.
    while (*tmp)
    {
        if (separator == *tmp)
        {
            count++;
            last_comma = tmp;
        }

        tmp++;
    }

    // Add space for trailing token.
    count += last_comma < (str + strlen(str) - 1);

    // Add space for terminating null string so caller
    // knows where the list of returned strings ends.
    count++;

    result = malloc(sizeof(char *) * count);

    if (!result) {
        perror("malloc");
    } else {
        size_t i = 0;
        char *token = strtok(str, delim);

        while (token)
        {
            result[i++] = strdup(token);
            token = strtok(NULL, delim);
        }

        result[i] = 0;
    }

    return result;
}