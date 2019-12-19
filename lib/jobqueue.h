#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>

#include "lib/utilities.h"

#define MAX_JOBS 10

typedef struct JobQueue JobQueue;
typedef struct Job Job;

struct Job {
    Buffer *executable;
    Buffer *input_file;
    char command[MAX_BUFFER_SIZE];
};

struct JobQueue {
    Job **jobs;
    int front;
    int rear;
    int capacity;
};

Job *createJob(int id, char *command);
JobQueue *createJobQueue(int capacity);
int size(int front, int rear, int capacity);
void enQueue(JobQueue *Q, int id, char *command);
Job *deQueue(JobQueue *Q);
Job *searchQueue(JobQueue *Q, int id);
void cleanupQueue(JobQueue *Q);

/**
 * Creates a given job given a ID and a command to be executed.
 *
 * WARNING: 'createJob' malloc()s memory to '*job' which must be freed by
 * the caller.
 *
 * @param id The ID of this job.
 * @param command The command send by the master to be executed.
 *
 * @return The struct representing this job.
 */
Job *createJob(int id, char *command) {
    Job *job = (Job *)malloc(sizeof(Job));

    if (!job) {
        perror("[X] malloc");
        exit(1);
    }

    job->executable = createBuffer();
    job->input_file = createBuffer();

    return job;
}

/**
 * Creates a queue of jobs with a given capacity.
 *
 * WARNING: 'createJobQueue' malloc()s memory to '*Q' which must be freed by
 * the caller.
 *
 * @param capacity The maximum number of jobs in this queue.
 *
 * @return The queue of jobs with a given capacity.
 */
JobQueue *createJobQueue(int capacity) {
    JobQueue *Q = (JobQueue *)malloc(sizeof(JobQueue));

    if (!Q) {
        perror("[X] malloc");
        exit(1);
    }

    Q->jobs = (Job **)malloc(sizeof(Job *) * capacity);

    for (int i = 0; i < capacity; i++)
        Q->jobs[i] = NULL;

    Q->front = -1;
    Q->rear = -1;
    Q->capacity = capacity;

    return Q;
}

/**
 * Determines the size of a given queue (thread safe).
 *
 * @param front The index of the front element of the queue.
 * @param rear The index of the rear element of the queue.
 * @param capacity The capacity of the queue.
 *
 * @return The size of the queue.
 */
int size(int front, int rear, int capacity)
{
    if (front == -1)
        return 0;

    if (front >= rear)
        return (rear + capacity) - front;

    return rear - front;
}

/**
 * Appends a new job to the end of the given queue.
 *
 * @param Q The queue that this job will be added too.
 * @param id The id of the job to be enqueued.
 * @param command The command of the job to be enqueued.
 */
void enQueue(JobQueue *Q, int id, char *command) {
    if (size(Q->front, Q->rear, Q->capacity) == 0) {
        Q->front = 0;
        Q->rear = 0;
    }

    Q->jobs[Q->rear++] = createJob(id, command);
    Q->rear %= Q->capacity;
}

/**
 * Removes the head of the queue.
 *
 * @param Q The given queue of jobs.
 *
 * @return The head of the queue.
 */
Job *deQueue(JobQueue *Q) {
    Job *job = Q->jobs[Q->front++];
    Q->front %= Q->capacity;

    if (Q->front == Q->rear)
        Q->front = -1;

    return job;
}

/**
 * Searches for a given job based on its ID within a given queue of jobs.
 *
 * @param Q The queue of jobs to be searched.
 * @param id The ID of the target job.
 *
 * @return The job that was found within the queue that has the given id.
 */
Job *searchQueue(JobQueue *Q, int id) {
    Job *job = NULL;

    for (int i = 0; i < Q->capacity; i++) {
        if (Q->jobs[i]->id == id) {
            job = Q->jobs[i];
            break;
        }
    }

    return job;
}

/**
 * Frees the memory created when 'createJobQueue' is called.
 *
 * @param Q The queue of job structs to be freed.
 */
void cleanupQueue(JobQueue *Q) {
    for (int i = 0; i < Q->capacity; i++)
        free(Q->jobs[i]);

    free(Q);
}
