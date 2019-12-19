/**
 * A C program used to count the number of words in a given text file.
 *
 * @author Nicholas Adamou
 * @author Jillian Shew
 * @author Bingzhen Li
 * @date 12/9/2019
 */

#include <stdio.h>
#include <stdlib.h>

#define OUTPUT_FILE_NAME "countwords_output.txt"

int countWords(char *file_path);
void write_to_file(int word_count);

/**
 * Counts the number of words in a given file.
 *
 * @param file_path The path to the file.
 *
 * @return The number of words contained in the given file.
 */
int countWords(char *file_path) {
    FILE *file = fopen(file_path, "r");

    if (!file) {
        fputs("[X] fopen: failed to open file.", stderr);
        exit(1);
    }

    char c;
    int count = 0;

    while ((c = fgetc(file)) != EOF) {
        if (c == '\n' || c == ' ')
            count++;
    }

    fclose(file);

    return count;
}

/**
 * Writes the number of words to a file.
 *
 * @param word_count The number of words found from countWords().
 */
void write_to_file(int word_count) {
    FILE *file = fopen(OUTPUT_FILE_NAME, "w+");

    if (!file) {
        fputs("[X] fopen: failed to open file.", stderr);
        exit(1);
    }

    fprintf(file, "%d", word_count);

    fclose(file);
}

int main(int argc, char **argv) {
    char *file_path;

    /* Get file-path from command line arguments or stdin. */
    file_path = argc > 1 ? argv[1] : 0;
    if (!file_path) {
        printf("Enter the path to the file: ");
        scanf("%s", file_path);
    }

    int count = countWords(file_path);
    write_to_file(count);

    return 0;
}