#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <time.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_CPUS 19

int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

int ncpus() {
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) {
        perror("Could not obtain hardware thread count. Defaulting to 1.");
        return 1;
    }
    return (int) n;
}

int capInt(int num, int cap) {
    return (num > cap) ? cap : num;
}

int floorInt(int num, int floor) {
    return (num < floor) ? floor : num;
}

// Accepts total number of jobs and available workers
// Returns integer list of number of jobs for each thread (0 for unused workers)
int * distributeJobs(int jobs, int workers) {
    int workloadBase = jobs / workers; // Number of files given to each of the threads
    int workloadExtra = jobs % workers; // Number of files left over after even distribution

    int * workloads = malloc(workers * sizeof(int));
    if (!workloads) {
        perror("Could not allocate memory.");
        return NULL;
    }

    // Distribute jobs across workers, adding 1 extra to the first workers to account for leftovers
    for (int i = 0; i < workers; i++) {
        workloads[i] = workloadBase + ((i < workloadExtra) ? 1 : 0);
    }

    return workloads;
}

typedef struct {
    unsigned char * buffer;
    int nbytes;
} CompressionBuffer;

typedef struct {
    char ** files;
    int start;
    int end;
    char * path;
    int * total_in;
    int * total_out;
    pthread_mutex_t * mutex;
    CompressionBuffer * compressionBuffers;
} CompressFilesSegmentArgs;

void * compressFilesSegment(void * argsp) {
    CompressFilesSegmentArgs * args = (CompressFilesSegmentArgs *) argsp;
    char ** files = args->files;
    int start = args->start;
    int end = args->end;
    char * path = args->path;
    int * total_in = args->total_in;
    int * total_out = args->total_out;
    pthread_mutex_t * mutex = args->mutex;
    CompressionBuffer * compressionBuffers = args->compressionBuffers;

    for(int cur = start; cur < end; cur++) {
        int len = strlen(path) + strlen(files[cur]) + 2;
        char *full_path = malloc(len*sizeof(char));
        assert(full_path != NULL);
        strcpy(full_path, path);
        strcat(full_path, "/");
        strcat(full_path, files[cur]);

        unsigned char buffer_in[BUFFER_SIZE];
        unsigned char buffer_out[BUFFER_SIZE];

        // load file
        FILE *f_in = fopen(full_path, "r");
        assert(f_in != NULL);
        int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
        fclose(f_in);
        pthread_mutex_lock(mutex);
        *total_in += nbytes;
        pthread_mutex_unlock(mutex);

        // zip file
        z_stream strm;
        int ret = deflateInit(&strm, 9);
        assert(ret == Z_OK);
        strm.avail_in = nbytes;
        strm.next_in = buffer_in;
        strm.avail_out = BUFFER_SIZE;
        strm.next_out = buffer_out;

        ret = deflate(&strm, Z_FINISH);
        assert(ret == Z_STREAM_END);

        // dump zipped file
        int nbytes_zipped = BUFFER_SIZE-strm.avail_out;
        // Write to buffer
        CompressionBuffer * curBuffer = &compressionBuffers[cur];
        curBuffer->buffer = malloc(nbytes_zipped * sizeof(unsigned char));
        memcpy(curBuffer->buffer, buffer_out, nbytes_zipped);
        curBuffer->nbytes = nbytes_zipped;

        pthread_mutex_lock(mutex);
        *total_out += nbytes_zipped;
        pthread_mutex_unlock(mutex);

        free(full_path);
    }
    return NULL;
}

void * writeCompressionBuffers(CompressionBuffer * compressionBuffers, FILE * f_out, int nbuffers) {
    for (int i = 0; i < nbuffers; i++) {
        fwrite(&compressionBuffers[i].nbytes, sizeof(int), 1, f_out);
        fwrite(compressionBuffers[i].buffer, sizeof(unsigned char), compressionBuffers[i].nbytes, f_out);
        free(compressionBuffers[i].buffer);
    }
    return NULL;
}

int main(int argc, char **argv) {
	// time computation header
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
	// end of time computation header

	// do not modify the main function before this point!

	assert(argc == 2);

	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;

	d = opendir(argv[1]);
	if(d == NULL) {
		printf("An error has occurred\n");
		return 0;
	}

    // NO SPEEDUP POSSIBLE IN INITIALIZATION OR DIRECTORY OPENING

	// create sorted list of PPM files
	while ((dir = readdir(d)) != NULL) {
		files = realloc(files, (nfiles+1)*sizeof(char *));
		assert(files != NULL);

		int len = strlen(dir->d_name);
		if(dir->d_name[len-4] == '.' && dir->d_name[len-3] == 'p' && dir->d_name[len-2] == 'p' && dir->d_name[len-1] == 'm') {
			files[nfiles] = strdup(dir->d_name);
			assert(files[nfiles] != NULL);

			nfiles++;
		}
	}
	closedir(d);
	qsort(files, nfiles, sizeof(char *), cmp);

    // NO SPEEDUP POSSIBLE IN LIST OBTAINING OR DIRECTORY READING

	// create a single zipped package with all PPM files in lexicographical order
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("video.vzip", "w");
	assert(f_out != NULL);

    // SPEEDUP POSSIBLE IN COMPRESSION - EACH FILE CAN OCCUPY A PRIVATE THREAD
    // PER ASSIGNMENT - NO MORE THAN 20 THREADS (inc. main, so 19), CPU HAS 4 CORES
    // IDEALLY - DIVIDE LIST OF FILES (LENGTH n) INTO MIN(CEIL(n/N_CPU_LPUS), 19) SUBLISTS
    // THEN LET A THREAD OWN EACH LIST. MAIN BLOCKED BY ALL, COMPOSING FULL LIST AT END

    // BEGIN SPEEDUP

    // Break workload into up to 19 threads
    int cpus = ncpus();
    int maxHardwareThreads = capInt(cpus, MAX_CPUS);
    int * workloads = distributeJobs(nfiles, maxHardwareThreads);

    // Create worker threads
    pthread_t * threads = malloc(maxHardwareThreads * sizeof(pthread_t));
    CompressFilesSegmentArgs * args = malloc(maxHardwareThreads * sizeof(CompressFilesSegmentArgs));
    // Create mutex for thread-safe writing to shared pointers
    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);
    // Allocate buffer to preserve order
    CompressionBuffer * compressionBuffers = malloc(nfiles * sizeof(CompressionBuffer));

    int cur = 0;
    for (int i = 0; i < maxHardwareThreads; i++) {
        args[i].files = files;
        args[i].start = cur;
        args[i].end = cur += workloads[i];
        args[i].path = argv[1];
        args[i].total_in = &total_in;
        args[i].total_out = &total_out;
        args[i].mutex = &mutex;
        args[i].compressionBuffers = compressionBuffers;

        if (pthread_create(&threads[i], NULL, compressFilesSegment, &args[i]) != 0) {
            perror("Could not create thread. Exiting.");
            exit(1);
        }
    }

    // Wait for all threads to finish
    for (int i = 0; i < maxHardwareThreads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Write buffers
    writeCompressionBuffers(compressionBuffers, f_out, nfiles);

    // END SPEEDUP

    // Cleanup
    free(threads);
    free(args);
    pthread_mutex_destroy(&mutex); // Destroy the mutex
	fclose(f_out);

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

	// release list of files
	for(cur=0; cur < nfiles; cur++)
		free(files[cur]);
	free(files);

    // NO SPEEDUP MEANINGFUL IN FILE RELEASING

	// do not modify the main function after this point!

	// time computation footer
	clock_gettime(CLOCK_MONOTONIC, &end);
	printf("Time: %.2f seconds\n", ((double)end.tv_sec+1.0e-9*end.tv_nsec)-((double)start.tv_sec+1.0e-9*start.tv_nsec));
	// end of time computation footer

	return 0;
}
