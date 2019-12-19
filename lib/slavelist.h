#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>

typedef struct Slave Slave;
typedef struct SlaveList SlaveList;

struct Slave {
    int id;
    char *address;
    float utilization;
};

struct SlaveList {
    Slave **slaves;
    int size;
    int capacity;
};

Slave *createSlave(char *address, int id);
SlaveList *createSlaveList(int capacity);
int add(SlaveList *list, char *address);
Slave *searchList(SlaveList *list, char *address);
void cleanupList(SlaveList *list);

/**
 * Creates a given slave based on its IP address.
 *
 * WARNING: 'createSlave' malloc()s memory to '*slave' which must be freed by
 * the caller.
 *
 * @param address The IP Address of this slave.
 * @param id The id of this slave.
 *
 * @return The struct representing this slave.
 */
Slave *createSlave(char *address, int id) {
    Slave *slave = (Slave *)malloc(sizeof(Slave));

    if (!slave) {
        perror("[X] malloc");
        exit(1);
    }

    slave->id = id;
    slave->address = address;
    slave->utilization = 1;

    return slave;
}

/**
 * Creates a list of slaves based on a given capacity.
 *
 * WARNING: 'createSlaveList' malloc()s memory to '*slaveList' which must be freed by
 * the caller.
 *
 * @param capacity The maximum number of slaves in this list.
 *
 * @return The list of slaves based on a given capacity.
 */
SlaveList *createSlaveList(int capacity) {
    SlaveList *slaveList = (SlaveList *)malloc(sizeof(SlaveList));

    if (!slaveList) {
        perror("[X] malloc");
        exit(1);
    }

    slaveList->slaves = (Slave **)malloc(sizeof(Slave *) * capacity);

    for (int i = 0; i < capacity; i++)
        slaveList->slaves[i] = NULL;

    slaveList->capacity = capacity;
    slaveList->size = 0;

    return slaveList;
}

/**
 * Given a list of slaves, adds a slave based on its IP Address to the list.
 *
 * @param list The list of slaves to be added to0.
 * @param address The IP Address of the slave to be added.
 *
 * @return The id of the slave.
 */
int add(SlaveList *list, char *address) {
   if (list->size != list->capacity - 1) {
        int index = list->size;
        Slave *slave = createSlave(address, index);
        list->slaves[list->size++] = slave;
        return index;
    }

    return -1;
}

/**
 * Searches for a given slave based on its IP Address within a given list of slaves.
 *
 * @param list The list of slaves to search in.
 * @param address The IP Address of the target slave.
 *
 * @return The slave that was found within the list that has the given IP Address.
 */
Slave *searchList(SlaveList *list, char *address) {
    Slave *slave = NULL;

    for (int i = 0; i < list->capacity; i++) {
        if (strcmp(list->slaves[i]->address, address) == 0) {
            slave = list->slaves[i];
            break;
        }
    }

    return slave;
}

/**
 * Frees the memory created when 'createSlaveList' is called.
 *
 * @param list The list of slaves structs to be freed.
 */
void cleanupList(SlaveList *list) {
    for (int i = 0; i < list->capacity; i++)
        free(list->slaves[i]);

    free(list);
}