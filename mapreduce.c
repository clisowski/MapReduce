#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <pthread.h>
#include "mapreduce.h"

struct pairs {
	char *key;
	char *value;
};

struct files {
	char *name;
};

struct pairs** partitions;
struct files* fileNames;
int* pairCount;
int* pairInPartition;
int* numberOfAccess;
pthread_mutex_t lock, fileLock;
Partitioner p;
Reducer r;
Mapper m;
int numberPartitions;
int filesProc;
int totalFiles;

// mapper helper func
void* mapperHelper(void *arg) {
	while(filesProc < totalFiles) {
		pthread_mutex_lock(&fileLock);
		char *filename = NULL;
		if(filesProc < totalFiles) {
			filename = fileNames[filesProc].name;
			filesProc++;
		}
		pthread_mutex_unlock(&fileLock);
		if(filename != NULL)
			m(filename);
	}
	return arg;
}

char* get_next(char *key, int partition_number) {
	int num = numberOfAccess[partition_number];
	if(num < pairCount[partition_number] && strcmp(key, partitions[partition_number][num].key) == 0) {
		numberOfAccess[partition_number]++;
		return partitions[partition_number][num].value;
	}
	else {
		return NULL;
	}
}


void* reducerHelper(void *arg) {
	int* partitionNumber = (int *)arg;
	for(int i = 0; i < pairCount[*partitionNumber]; i++) {
		if(i == numberOfAccess[*partitionNumber]) {
			r(partitions[*partitionNumber][i].key, get_next, *partitionNumber);
		}
	}
	return arg;
}


int compare(const void* p1, const void* p2) {
	struct pairs *pair1 = (struct pairs*) p1;
	struct pairs *pair2 = (struct pairs*) p2;
	if(strcmp(pair1->key, pair2->key) == 0) {
		return strcmp(pair1->value, pair2->value);
	}
	return strcmp(pair1->key, pair2->key);
}

// Sort files by increasing size
int compareFiles(const void* p1, const void* p2) {
	struct files *f1 = (struct files*) p1;
	struct files *f2 = (struct files*) p2;
	struct stat st1, st2;
	stat(f1->name, &st1);
	stat(f2->name, &st2);
	long int size1 = st1.st_size;
	long int size2 = st2.st_size;
	return (size1 - size2);
}

void MR_Emit(char *key, char *value) {
	pthread_mutex_lock(&lock); 

	unsigned long hashPartitionNumber = p(key, numberPartitions);
	pairCount[hashPartitionNumber]++;
	int curCount = pairCount[hashPartitionNumber];
	if (curCount > pairInPartition[hashPartitionNumber]) { //allocate more memory if necessary
		pairInPartition[hashPartitionNumber] *= 2;
		partitions[hashPartitionNumber] = (struct pairs *) realloc(partitions[hashPartitionNumber], pairInPartition[hashPartitionNumber] * sizeof(struct pairs));
	}
	partitions[hashPartitionNumber][curCount-1].key = (char*)malloc((strlen(key)+1) * sizeof(char));
	strcpy(partitions[hashPartitionNumber][curCount-1].key, key);
	partitions[hashPartitionNumber][curCount-1].value = (char*)malloc((strlen(value)+1) * sizeof(char));
	strcpy(partitions[hashPartitionNumber][curCount-1].value, value);
	pthread_mutex_unlock(&lock); 
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {

	if(argc - 1 < num_mappers) {
		num_mappers = argc - 1; // checks to make sure the number of files is less than number of mappers
	}

	//Initialising vars
	pthread_t mapperThreads[num_mappers];
	pthread_t reducerThreads[num_reducers];
	pthread_mutex_init(&lock, NULL);
	pthread_mutex_init(&fileLock, NULL);
	p = partition;
	m = map;
	r = reduce;
	numberPartitions = num_reducers;
	partitions = malloc(num_reducers * sizeof(struct pairs*));
	fileNames = malloc((argc-1) * sizeof(struct files));
	pairCount = malloc(num_reducers * sizeof(int));
	pairInPartition = malloc(num_reducers * sizeof(int));
	numberOfAccess = malloc(num_reducers * sizeof(int));
	filesProc = 0;
	totalFiles = argc - 1;
	int arrayPosition[num_reducers];

	// array initialization for key pair
	for(int i = 0; i < num_reducers; i++) {
		partitions[i] = malloc(1024 * sizeof(struct pairs));
		pairCount[i] = 0;
		pairInPartition[i] = 1024;
		arrayPosition[i] = i;
		numberOfAccess[i] = 0;
	}

	// files being copied for sort
	for(int i = 0; i <argc-1; i++) {
		fileNames[i].name = malloc((strlen(argv[i+1])+1) * sizeof(char));
		strcpy(fileNames[i].name, argv[i+1]);
	}

	// shortest file first in sort
	qsort(&fileNames[0], argc-1, sizeof(struct files), compareFiles);

	// thread creation for mappers
	for (int i = 0; i < num_mappers; i++) {
		pthread_create(&mapperThreads[i], NULL, mapperHelper, NULL);
	}

	// threads must finish
	for(int i = 0; i < num_mappers; i++) {
		pthread_join(mapperThreads[i], NULL); 
	}

	// Partition sort
	for(int i = 0; i < num_reducers; i++) {
		qsort(partitions[i], pairCount[i], sizeof(struct pairs), compare);
	}

	for (int i = 0; i < num_reducers; i++){
	    if(pthread_create(&reducerThreads[i], NULL, reducerHelper, &arrayPosition[i])) {
	    	printf("Error\n");
	    }
	}

	//Waiting for the threads to finish
	for(int i = 0; i < num_reducers; i++) {
		pthread_join(reducerThreads[i], NULL); 
	}

	pthread_mutex_destroy(&lock);
	pthread_mutex_destroy(&fileLock);

	for(int i = 0; i < num_reducers; i++) {
		// Free keys and values
		for(int j = 0; j < pairCount[i]; j++) {
			if(partitions[i][j].key != NULL && partitions[i][j].value != NULL) {
				free(partitions[i][j].key);
		    	free(partitions[i][j].value);
			}
		}
		// Free pair struct array
		free(partitions[i]);
	}

	// Free filenames
	for(int i = 0; i < argc-1; i++) {
		free(fileNames[i].name);
	}

	// Free memory
	free(partitions);
	free(fileNames);
	free(pairCount);
	free(pairInPartition);
	free(numberOfAccess);
}

// Given default hash function
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
