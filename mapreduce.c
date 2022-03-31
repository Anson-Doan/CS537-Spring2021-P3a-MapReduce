#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include "mapreduce.h"
#include "hashmap.h"

struct kv {
    char* key;
    char* value;
};

struct kv_list {
    struct kv** elements;
    size_t num_elements;
    size_t size;
};

size_t* kvl_counters;
struct kv_list** partitions;
size_t n_partitions;
Partitioner partitioner;
pthread_mutex_t* p_mutex_plural;
pthread_mutex_t bottleneck; // TODO delete this once we resolve problems with the array

struct kv_list* init_kv_list(size_t size) {
    struct kv_list* kvl = (struct kv_list*) malloc(sizeof(struct kv_list*));
    kvl->elements = (struct kv**) malloc(size * sizeof(struct kv*));
    kvl->num_elements = 0;
    kvl->size = size;

    return kvl;
}

void add_to_list(struct kv* elt) {
    int part_id = (*partitioner)(elt->key, n_partitions);

    //printf("locking partition %i\n", part_id);
    pthread_mutex_lock(&p_mutex_plural[part_id]);
    //printf("locked\n");
    //pthread_mutex_lock(&bottleneck); // TODO delete this once we resolve problems with the array
    
    if (partitions[part_id]->num_elements == partitions[part_id]->size) {
        partitions[part_id]->size *= 2;
        partitions[part_id]->elements = realloc(partitions[part_id]->elements, partitions[part_id]->size * sizeof(struct kv*));
    }
    partitions[part_id]->elements[partitions[part_id]->num_elements++] = elt;

    //printf("unlocking partition %i\n", part_id);
    pthread_mutex_unlock(&p_mutex_plural[part_id]);
    //printf("unlocked\n");
    //pthread_mutex_unlock(&bottleneck); // TODO delete this once we resolve problems with the array
}

char* get_func(char* key, int partition_number) {
    if (kvl_counters[partition_number] == partitions[partition_number]->num_elements) {
	return NULL;
    }
    struct kv *curr_elt = partitions[partition_number]->elements[kvl_counters[partition_number]];
    if (!strcmp(curr_elt->key, key)) {
	kvl_counters[partition_number]++;
	return curr_elt->value;
    }
    return NULL;
}

int cmp(const void* a, const void* b) {
    char* str1 = (*(struct kv **)a)->key;
    char* str2 = (*(struct kv **)b)->key;
    return strcmp(str1, str2);
}

void MR_Emit(char* key, char* value) {
    struct kv *elt = (struct kv*) malloc(sizeof(struct kv));
    if (elt == NULL) {
	printf("Malloc error! %s\n", strerror(errno));
	exit(1);
    }
    elt->key = strdup(key);
    elt->value = strdup(value);
    add_to_list(elt);

    return;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

struct map_args {
    Mapper map;
    int num_args;
    char** argv;
}map_args;

pthread_mutex_t mutex;

void *map_thread(void *arg)
{
    struct map_args *mapper_args;
    mapper_args = (struct map_args *) arg;
    Mapper map = mapper_args->map;
    int num_args = mapper_args->num_args;

    for (int i = 0; i < num_args; i++) {
	    (*map)(mapper_args->argv[i]);
    }

    return NULL;
}

// REDUCERS helper functions

struct reduce_args {
    Reducer reduce;
    int p_id;
} reduce_args;

pthread_mutex_t r_mutex;

void *reduce_thread(void *arg) {
    struct reduce_args *reducer_args;
    reducer_args = (struct reduce_args *) arg;
    Reducer reduce = reducer_args->reduce;
    int p_id = reducer_args->p_id;

    while (kvl_counters[p_id] < partitions[p_id]->num_elements) {
	    (*reduce)(partitions[p_id]->elements[kvl_counters[p_id]]->key, get_func, p_id);
    }

    return NULL;
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
	    Reducer reduce, int num_reducers, Partitioner partition) {

    n_partitions = num_reducers;
    partitions = (struct kv_list**) malloc(n_partitions * sizeof(struct kv_list*));
    partitioner = partition;

    for (int i = 0; i < n_partitions; i++) {
        partitions[i] = init_kv_list(10);
    }

    p_mutex_plural = (pthread_mutex_t*) malloc(n_partitions * sizeof(pthread_mutex_t));

    for (int i = 0; i < n_partitions; i++) {
        pthread_mutex_init(&p_mutex_plural[i], NULL);
    }

    kvl_counters = malloc(n_partitions * sizeof(size_t));
    for (int i = 0; i < n_partitions; i++) {
        kvl_counters[i] = 0;
    }

    pthread_t thread_ids[num_mappers];

    char* args_per_thread[num_mappers][argc - 1];
    int num_args[num_mappers];

    // NEVER delete
    for (int i = 0; i < num_mappers; i++) {
        num_args[i] = 0;
    }

    int thread_count = 0;
    int place_count = 0;
    
    // iterates through files, assigns them to threads
    for (int i = 1; i < argc; i++) {
        
        char* argv_ptr = argv[i];
        args_per_thread[thread_count][place_count] = argv_ptr;
        num_args[thread_count] += 1;

        thread_count++;

        if (thread_count >= num_mappers) {
            thread_count = 0;
            place_count++;
        }
    }

    // array for pthread create calls
    struct map_args* result[num_mappers];

    // populate thread arguments
    for (int i = 0; i < num_mappers; i++) {
        struct map_args* mapper_args;
    
        mapper_args = malloc(sizeof(map_args));
        mapper_args->map = map;
        mapper_args->argv = args_per_thread[i];
        mapper_args->num_args = num_args[i];

        result[i] = mapper_args;
    }

    //creates threads
    for (int i = 0; i < num_mappers; i++){
        pthread_create(&thread_ids[i], NULL, map_thread, result[i]);
    }

    // wait for threads to finish
    for (int i = 0; i < num_mappers; i++) {
        pthread_join(thread_ids[i], NULL);
    }

    for (int i = 0; i < n_partitions; i++) {
        pthread_mutex_destroy(&p_mutex_plural[i]);
    }

    // REDUCERS

    pthread_t r_thread_ids[num_reducers];

    // Sorts the elements so they are in order like specified
    for (int i = 0; i < n_partitions; i++) {
        qsort(partitions[i]->elements, partitions[i]->num_elements, sizeof(struct kv*), cmp);
        
    }

    // array for pthread create calls
    struct reduce_args* r_result[num_reducers];

    // populate thread arguments
    for (int i = 0; i < num_reducers; i++) {
        struct reduce_args* reducer_args;
    
        reducer_args = malloc(sizeof(reduce_args));
        reducer_args->reduce = reduce;
        reducer_args->p_id = i;

        r_result[i] = reducer_args;
    }

    //creates threads
    for (int i = 0; i < num_reducers; i++){
        pthread_create(&r_thread_ids[i], NULL, reduce_thread, r_result[i]);
    }

    // wait for threads to finish
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(r_thread_ids[i], NULL);
    }
}
