#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#define INDEX 1024

// Data structure that holds the physical chunks of data in dataChunk arrays
struct dataChunks
{
    int dataChunk[INDEX];
    struct dataChunks *next;
};

// Data structure that holds pointers to the physical data chunks along with their ID
struct descriptor
{
    int chunkID;
    int *dataChunkPtr;
    int chunkState;
    struct descriptor *next;
};

// Data structure that holds the two variables passed to the mergeChunks function
typedef struct mergeRunArgs
{
    int phase;
    int indexOfFile;
} mergeArgs;

// Data structure that holds information of each thread
typedef struct thread_data_t
{
    int thread_num;
} thread_data_t;

typedef struct dataChunks dataChunks;
typedef struct descriptor descriptor;

dataChunks dataChunksHead;
descriptor *chunkListStart, *chunkListEnd;
thread_data_t *thread_info;

// Set up the clock
struct timeval start_time[1];
struct timeval current_time[1];
struct timeval finish_time[1];
unsigned long diff = 0;

// Character arrays used to produce file names
const char filenames[] = "sorted.out";

// Global declarations of the number of threads and the number of cores they are bound to
int numThreads, numCores;

// Function headers for the functions used in the program
void merge(int *v, int p, int q, int r); // Function to implement merge sort
void mergeSort(int *v, int p, int r); // Sorts the chunk using merge sort
int initializeChunks(); // Initializes the data chunks
int fileLength(FILE *f); // Returns the number of items in a file
void createFileName(char *num, int indx, int phs); // Creates file names for output files
void outputToFiles(char *num, int num1, int *ptr); // Sorted chunks are output to files
void mergeChunks(int phase, int indexOfFile); // Merges two files into one larger output file
int power(int base, int power); // Calculates the base "to the power" power
void *run(void *thread_args); // Function that the worker threads use to sort the data chunks
void *mergeRun(void *arg); // Function that the merging threads use to merge the chunks into one sorted file
int limit_cores(pthread_t *thread, int num_cores); // Function used to bind threads to cores
void makeInFile(); // Function used for file i/o
void removeInFile(); // Function used for file i/o
void makeOutFile(int phase); // Function used for file i/o

// Declaration of the mutex lock to lock the descriptor list
pthread_mutex_t mutex;

// Declaration of a barrier used while merging two files into one
pthread_barrier_t barrier;

int main (int argc, char *argv[])
{

    if(argc != 3)
    {
        printf("Usage: %s NUM_THREADS NUM_CORES\n", argv[0]);
        return 0;
    }

    numThreads = atoi (argv[1]);
    numCores = atoi (argv[2]);

    thread_info = malloc(sizeof(thread_data_t) * numThreads);

    pthread_t threads[numThreads];

    pthread_mutex_init(&mutex, NULL);

    makeInFile();

    // Pointer declaration to the input and output files
    FILE *myfile;

    // Open readable input file and writable output file
    myfile = fopen("data.in","r");

    // Check to see if the files exist
    if (myfile == NULL)
    {
        printf("File does not exist\n");
        return 0;
    }

    // Calculate the length of the file
    int lengthOfFile, numOfChunks;
    lengthOfFile = fileLength(myfile);
    numOfChunks = lengthOfFile/INDEX;

    printf("\nRead in %i data chunks\n", numOfChunks);
    initializeChunks();

    int i;

    gettimeofday(start_time, NULL);

    printf("Create %i threads to sort the data chunks\n", numThreads);
    printf("Binding %i threads to %i cores\n", numThreads, numCores);

    for( i = 0 ; i < numThreads ; i ++)
    {
        thread_info[i].thread_num = i;
        (void) pthread_create(&threads[i], NULL, run, &thread_info[i]);
        limit_cores(&threads[i], numCores);
    }

    for( i = 0 ; i < numThreads ; i ++)
    {
        (void) pthread_join(threads[i], NULL);
    }

    numThreads = numOfChunks/2;
    pthread_t mergingThreads[numThreads];

    printf("\nStarting merge stage\n");
    printf("Creating %i merging threads\n", numThreads);
    printf("Binding threads to %i CPU cores\n", numCores);

    int phase = 1;

    while(numThreads >= 1)
    {

        pthread_barrier_init(&barrier, NULL, numThreads+1);

        for( i = 0 ; i < numThreads ; i ++)
        {
            thread_info[i].thread_num = i;
            mergeArgs *args = (mergeArgs *)malloc(sizeof(mergeArgs));
            args->phase = phase;
            args->indexOfFile = i+1;
            pthread_create(&mergingThreads[i], NULL, mergeRun, (void *)args);
            limit_cores(&mergingThreads[i], numCores);
            gettimeofday(current_time, NULL);
            printf("Timestamp: %lu ", (current_time->tv_sec)*1000000 + (current_time->tv_usec));
            printf("Merging thread %i starts working on files in phase %i\n", thread_info[i].thread_num, args->phase);
        }

        pthread_barrier_wait(&barrier);

        pthread_barrier_destroy(&barrier);

        numThreads = numThreads/2;
        phase++;
    }

    gettimeofday(finish_time, NULL);
    diff = (finish_time->tv_sec - start_time->tv_sec) * 1000000 + (finish_time->tv_usec - start_time->tv_usec);

    removeInFile();
    makeOutFile(phase);

    printf("All threads finished\n");
    printf("The main thread exits\n");
    printf("The program finished in %lu micro seconds\n\n", diff);

    return 0;
}

// ALL FUNCTION DEFINITIONS

void merge(int *v, int p, int q, int r)
{

    int i = p;
    int j = q + 1;

    int *tmp = (int*)malloc((r - p + 1) * sizeof(int));
    int k = 0;

    while ((i <= q) && (j <= r))
    {
        if (v[i] < v[j])
            tmp[k++] = v[i++];
        else
            tmp[k++] = v[j++];
    }

    while (i <= q)
        tmp[k++] = v[i++];

    while (j <= r)
        tmp[k++] = v[j++];

    memcpy(v + p, tmp, (r - p + 1) * sizeof(int));
    free(tmp);
}

void mergeSort(int *v, int p, int r)
{

    if (p < r)
    {
        int q = (p + r) / 2;
        mergeSort(v, p, q);
        mergeSort(v, q + 1, r);
        merge(v, p, q, r);
    }
}

int fileLength(FILE *f)
{

    int s, t, u = 0;
    while ((s = fgetc(f)) != EOF)
    {
        fscanf(f, "%i", &t);
        u++;
    }
    rewind(f);
    return u;
}

int initializeChunks()
{

    dataChunksHead.next = NULL;

    chunkListStart = (descriptor*)malloc(sizeof(descriptor));
    chunkListEnd = (descriptor*)malloc(sizeof(descriptor));

    // Pointer declaration to the input and output files
    FILE *infile;
    FILE *myfile;

    // Open readable input file and writable output file
    infile = fopen("newdata.in","r");

    // Check to see if the files exist
    if (infile == NULL)
    {
        printf("File does not exist\n");
        return 0;
    }

    // Calculate the length of the file
    int lengthOfFile, numOfChunks;
    lengthOfFile = fileLength(infile);
    numOfChunks = lengthOfFile/INDEX;

    // Read data from file into a data chunk array which is added to the dataChunks list
    int temp2, input, j=0, id = 1;
    fpos_t pos;

    for (temp2 = 0; temp2<numOfChunks; temp2++)
    {

        dataChunks *tempPtr;

        for(tempPtr = &dataChunksHead; tempPtr->next != NULL; tempPtr = tempPtr->next);
        tempPtr->next = (dataChunks*)malloc(sizeof(dataChunks));

        if (temp2 != 0)
        {
            fsetpos(infile, &pos);
        }

        while (((input = fgetc(infile)) != EOF) && (j<INDEX))
        {
            tempPtr->next->next = NULL;
            fscanf(infile, "%i", &tempPtr->next->dataChunk[j++]);
            fgetpos(infile, &pos);
        }

        // A pointer to the data chunk array is added to the chunkList list
        descriptor *tempPtr1;

        for(tempPtr1 = chunkListStart; tempPtr1->next != NULL; tempPtr1 = tempPtr1->next);
        tempPtr1->next = (descriptor*)malloc(sizeof(descriptor));

        tempPtr1->next->next = NULL;
        tempPtr1->next->chunkID = id++;
        tempPtr1->next->chunkState = 0;
        tempPtr1->next->dataChunkPtr = tempPtr->next->dataChunk;

        j=0;
    }

    // Close input file
    fclose(infile);
}

void createFileName(char *num, int indx, int phs)
{
    char phase[4];
    sprintf(num, "%d", indx);
    strcat(num, filenames);
    sprintf(phase, "%d", phs);
    strcat(num, phase);
}

void outputToFiles(char *num, int num1, int *ptr)
{

    createFileName(num, num1, 0);

    // Create an output file and open it to save the data chunks
    FILE *outfile;
    outfile = fopen(num, "w");

    int counter;

    // Save the sorted chunk to file
    for (counter=0; counter<INDEX; counter++)
    {
        fprintf(outfile, ",%i", *(ptr+counter));
    }

    // Close output file
    fclose(outfile);
}

void mergeChunks(int phase, int indexOfFile)
{

    int oldSize = power(2, phase-1)*INDEX;
    int newSize = 2 * oldSize;

    int store[newSize];
    char out1[30];
    char out2[30];
    char newout[30];
    int pt1, pt2, counter;

    int a = 0;

    FILE *outPtrs[2];

    createFileName(out1, 2*indexOfFile-1, phase-1);
    createFileName(out2, 2*indexOfFile, phase-1);
    createFileName(newout, indexOfFile, phase);

    outPtrs[0] = fopen(out1, "r");
    outPtrs[1] = fopen(out2, "r");

    if(outPtrs[0] == NULL)
        printf("The file does not exist\n");
    if(outPtrs[1] == NULL)
        printf("The file does not exist\n");

    while(((pt1 = fgetc(outPtrs[0]))!=EOF) && (a < oldSize))
        fscanf(outPtrs[0], "%i", &store[a++]);

    while(((pt2 = fgetc(outPtrs[1]))!=EOF) && (a < newSize))
    {
        fscanf(outPtrs[1], "%i", &store[a++]);
    }

    mergeSort(store, 0, newSize - 1);

    fclose(outPtrs[0]);
    fclose(outPtrs[1]);

    FILE *trialfile;

    trialfile = fopen(newout, "w");

    for (counter = 0; counter < newSize; counter++)
        fprintf(trialfile, ",%i", *(store+counter));

    fclose(trialfile);

    remove(out1);
    remove(out2);
}

int power(int base, int power)
{

    int oldBase = base;
    int i;

    if (power == 0)
    {
        return 1;
    }
    for (i=1; i<power; i++)
    {
        base = base * oldBase;
    }

    return base;
}

void *run(void *thread_args)
{

    while(1)
    {

        int thread_num = ((thread_data_t*)thread_args)->thread_num;

        pthread_mutex_lock(&mutex);

        descriptor *ptr = chunkListStart->next;

        if (ptr == NULL)
        {
            printf("No more work to do. Thread %i exiting\n", thread_num);
            pthread_mutex_unlock(&mutex);
            pthread_exit(NULL);
        }

        chunkListStart->next = ptr->next;
        pthread_mutex_unlock(&mutex);

        char outputfilenum[20];

        gettimeofday(current_time, NULL);
        printf("Timestamp: %lu ", (current_time->tv_sec)*1000000 + (current_time->tv_usec));

        printf("Thread %i starts working on chunk %i\n", thread_num, ptr->chunkID);

        mergeSort(ptr->dataChunkPtr, 0, INDEX-1);
        outputToFiles(outputfilenum, ptr->chunkID, ptr->dataChunkPtr);

        gettimeofday(current_time, NULL);
        printf("Timestamp: %lu ", (current_time->tv_sec*1000000) + (current_time->tv_usec));

        printf("Thread %i finished working on chunk %i\n", thread_num, ptr->chunkID);

    }
}

void *mergeRun(void *arg)
{

    mergeArgs *a = (mergeArgs*)arg;
    mergeChunks(a->phase, a->indexOfFile);
    pthread_barrier_wait(&barrier);
}

int limit_cores(pthread_t *thread, int num_cores)
{

    int s, j;
    cpu_set_t cpuset;

    // Set affinity mask to include CPUs 0 to (num_cores-1)

    CPU_ZERO(&cpuset);
    for (j = 0; j < num_cores; j++)
        CPU_SET(j, &cpuset);

    s = pthread_setaffinity_np(*thread, sizeof(cpu_set_t), &cpuset);

    if(s != 0)
    {
        printf("Error %i in pthread_setaffinity_np()...\n", s);
        return -1;
    }

    // Check the actual affinity mask assigned to the thread
    s = pthread_getaffinity_np(*thread, sizeof(cpu_set_t), &cpuset);

    if(s != 0)
    {
        printf("Error %i in pthread_getaffinity_np()...\n", s);
        return -1;
    }

    // Check to see how many cores are assigned to the thread.
    int count = 0;
    for (j = 0; j < CPU_SETSIZE; j++)
        if (CPU_ISSET(j, &cpuset))
            count++;

    if(count > num_cores)
    {
        printf("Error, the thread seems to be using more cores than you requested...\n");
        return -2;
    }

    return 0;
}

void makeInFile()
{

    FILE *myfile = fopen("data.in","r");

    FILE *newfile = fopen("newdata.in", "w");
    fprintf(newfile, ",");

    int c;
    while((c=fgetc(myfile))!=EOF)
    {
        fputc(c,newfile);
    }

    fclose(myfile);
    fclose(newfile);
}

void removeInFile()
{

    remove("newdata.in");
}

void makeOutFile (int phs)
{

    char lastFile[20];
    int p;

    p = phs-1;

    createFileName(lastFile, 1, p);

    FILE *filePtr1 = fopen(lastFile,"r");

    FILE *filePtr2 = fopen("data_sort.out", "w");

    int ch;
    int dummy;

    dummy = fgetc(filePtr1);

    while ( (ch=fgetc(filePtr1)) != EOF)
    {

        fputc(ch, filePtr2);
    }

    fclose(filePtr1);
    fclose(filePtr2);

}
