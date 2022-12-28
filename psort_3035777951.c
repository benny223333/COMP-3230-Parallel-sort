// COMP3230 Programming Assignment Two
// The parallel version of the sorting using qsort

/*
# Filename: psort_3035777951.c
# Student name and No.: Benny Leung 3035777951
# Development platform: gcc (Ubuntu 9.4.0-1ubuntu1~20.04.1) 9.4.0 on WSL2
# Remark: All requirements an bonus completed
*/

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>
#include <sys/fcntl.h>
#include <memory.h>
#include <math.h>
#include <limits.h>

int checking(unsigned int *, long);
int compare(const void *, const void *);

/////////  Global variables  /////////
// used in phase 2
unsigned int *MT_p2_pivots;    // stores all pivots from work thread in p2, will be freed after p-1 pivots is selected (p2 end)
int Mt_p2_samples_counter = 0; // to mark the next empty space in MT_p2_pivots
// used in phase 3 and 4
unsigned int *pivots;      // stores the pivots selected, will be freed after parition merging complete(p4 end)
unsigned int **partitions; // stores each partition from all subsequence, the first element of each partition is its size
int *partitions_size;      // stores the size for each partition

/////////  User's input and base array to be sorted  /////////
long size;            // size of the intarr, how many numbers
int thread_no;        // how many worker threads
unsigned int *intarr; // array of random integers

/////////  Synchronization tools  /////////
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_var = PTHREAD_COND_INITIALIZER;
int p1_complete_counter = 0; // to keep track how many thread finish phase 2

/////////  Structure to build arguments to be passed to thread function  /////////
struct p1_thread_arg
{
  int thread_id;    // stores which thread it is, only for debug purpose in p1
  long subseq_size; // stores the length of the subsequence
  long start_pos;   // stores the start position of the subseqence in intarr
  long end_pos;     // stores the end position of the subseqence in intarr
};

/////////  Thread function /////////
void *workerT_p1_sort(void *p1_thread_arg)
{
  // p1 variable initialize
  struct p1_thread_arg *arg = p1_thread_arg;
  qsort(&intarr[arg->start_pos], arg->subseq_size, sizeof(unsigned int), compare); // p1 complete
  //  pivots are store in MT_p2_pivots with Mt_p2_samples_counter keep track of the next free slot to fill,
  //  to avoid the counter messed up, a mutex lock is used
  //  the order does not matter, what matters is new values does not overwrite old values and vice versa
  pthread_mutex_lock(&mutex);
  for (int i = 1; i <= thread_no; i++)
  {
    unsigned int pivot = intarr[arg->start_pos + (((i - 1) * arg->subseq_size) / (thread_no * thread_no))];
    MT_p2_pivots[Mt_p2_samples_counter] = pivot;
    Mt_p2_samples_counter++;
  }
  // to wake up main thread after all worker thread finish p1
  p1_complete_counter++;
  if (p1_complete_counter == thread_no) // only signal main thread when all worker thread finish
    pthread_cond_signal(&cond_var);
  pthread_mutex_unlock(&mutex);

  // p3 variable initialize
  unsigned int partition_end_pivot;
  pthread_mutex_lock(&mutex); // wait for main thread to select pivots
  while (!pivots)
    pthread_cond_wait(&cond_var, &mutex);

  // Partition code logic: read through its own subsequence, and submit each number to the correct partition arrary
  // The partition array is an array of array, such that the 1st index indicates the partition number, and 2nd indicates the position of a number
  // Assumption: the subsequence was sorted locally, so the next number will always > current number
  // if next number should be in next partition, all number after it should also be next, or even later partition

  int j = 0; // current partition, also used for indicate the ending pivot of current partition, e.g. j = 1 means partition 1 and pivots[1]
  partition_end_pivot = pivots[j];

  for (long i = 0; i < arg->subseq_size; i++)
  {
    if (intarr[arg->start_pos + i] > partition_end_pivot) // current number > pivot, move to the next pivot(next partition)
    {
      if (j == thread_no - 2) // the current pivot is the last pivot
      {
        partition_end_pivot = INT_MAX; // set to max val of int so that all remainig numbers will go into the last partition
        j++;                           // move on to the next partition
      }
      else
      {
        j++;                             // move on to the next partition
        partition_end_pivot = pivots[j]; // next pivot
      }
      partitions_size[j]++;                                                                // current partition size++
      partitions[j] = realloc(partitions[j], partitions_size[j] * sizeof(unsigned int *)); // expand the partition space by 1
      partitions[j][partitions_size[j] - 1] = intarr[arg->start_pos + i];                  // add number
    }
    else if (intarr[arg->start_pos + i] == partition_end_pivot) // current number == pivot, add number and move on
    {
      partitions_size[j]++;                                                                // current partition size++
      partitions[j] = realloc(partitions[j], partitions_size[j] * sizeof(unsigned int *)); // expand the partition space
      partitions[j][partitions_size[j] - 1] = intarr[arg->start_pos + i];                  // add number
      if (j == thread_no - 2)
      {
        partition_end_pivot = INT_MAX; // set to max val of int so that all remainig numbers will go into the last partition
        j++;                           // move on to the next partition
      }
      else
      {
        j++;                             // move on to the next partition
        partition_end_pivot = pivots[j]; // next pivot
      }
    }
    else if (intarr[arg->start_pos + i] < partition_end_pivot) // current number < pivot, add number only
    {
      partitions_size[j]++;                                                                // current partition size++
      partitions[j] = realloc(partitions[j], partitions_size[j] * sizeof(unsigned int *)); // expand the partition space
      partitions[j][partitions_size[j] - 1] = intarr[arg->start_pos + i];                  // add number
    }
  }
  pthread_mutex_unlock(&mutex);
  return NULL;
}

void parallel_sort(int thread_no, long size)
{
  // This is the main thread
  // initialize all required variables
  pthread_t *workerTs = malloc(thread_no * sizeof(pthread_t));
  struct p1_thread_arg *p1_args = malloc(thread_no * sizeof(struct p1_thread_arg));
  long subseq_size = size / thread_no;
  // initialize arrays to be used later, so that the for loop below can initialize everything, and no other for loop is need to initialize things
  MT_p2_pivots = (unsigned int *)malloc((thread_no * thread_no) * sizeof(unsigned int)); // needed right after worker thread qsorted their own subseuence
  partitions = (unsigned int **)malloc((thread_no) * sizeof(unsigned int *));
  partitions_size = (int *)malloc((thread_no) * sizeof(int));
  for (int i = 0; i < thread_no; i++)
  {
    p1_args[i].subseq_size = subseq_size;
    if (i == (thread_no - 1))
    {
      if (subseq_size * thread_no != size) // if there is any number left, put all of them to the last thread
      {
        long dif = subseq_size + (size - (subseq_size * thread_no));
        p1_args[i].subseq_size = dif;
      }
    }
    p1_args[i].start_pos = i * subseq_size;
    p1_args[i].end_pos = (i + 1) * subseq_size;
    p1_args[i].thread_id = i;
    partitions[i] = (unsigned int *)malloc(0);
    partitions_size[i] = 0;
    pthread_create(&workerTs[i], NULL, workerT_p1_sort, &p1_args[i]);
  }

  // wait for all thread to finish submit pivots
  pthread_mutex_lock(&mutex);
  if (p1_complete_counter < thread_no)
    pthread_cond_wait(&cond_var, &mutex);
  pthread_mutex_unlock(&mutex);
  // p1 finish

  // p2, to sort the pivot array and select pivots for partition
  pthread_mutex_lock(&mutex);
  qsort(MT_p2_pivots, Mt_p2_samples_counter - 1, sizeof(unsigned int), compare);
  pivots = (unsigned int *)malloc((thread_no - 1) * sizeof(unsigned int)); // only p - 1 pivots will be selceted
  int temp = floor(thread_no / 2);
  for (int i = 1; i < thread_no; i++)
    pivots[i - 1] = MT_p2_pivots[((i)*thread_no) + temp - 1];
  free(MT_p2_pivots);
  pthread_cond_broadcast(&cond_var); // wake all worker threads
  pthread_mutex_unlock(&mutex);

  for (int i = 0; i < thread_no; i++)
    pthread_join(workerTs[i], NULL);

  // sort all partitions
  for (int i = 0; i < thread_no; i++)
    qsort(partitions[i], partitions_size[i], sizeof(unsigned int), compare);

  long intarr_marker = 0;
  for (int i = 0; i < thread_no; i++) // put the partitions back in intarr
  {
    for (long j = 0; j < partitions_size[i]; j++)
    {
      intarr[intarr_marker] = partitions[i][j];
      intarr_marker++;
    }
  }
}

int main(int argc, char **argv)
{
  long i;
  struct timeval start, end;
  // printf("program start\n");
  if ((argc != 2 && argc != 3))
  {
    printf("Usage: psort <number> [<no_of_workers>]\n");
    exit(0);
  }
  if (argv[2] == NULL)
    thread_no = 4;
  else if (atol(argv[2]) != 1)
    thread_no = atol(argv[2]);
  else
  {
    printf("Error: no. of worker threads must be greater than 1.\n");
    exit(0);
  }

  size = atol(argv[1]);
  intarr = (unsigned int *)malloc(size * sizeof(unsigned int));
  if (intarr == NULL)
  {
    perror("malloc");
    exit(0);
  }

  // set the random seed for generating a fixed random
  // sequence across different runs
  char *env = getenv("RANNUM"); // get the env variable
  if (!env)                     // if not exists
    srandom(3230);
  else
    srandom(atol(env));

  for (i = 0; i < size; i++)
  {
    intarr[i] = random();
  }

  // measure the start time
  gettimeofday(&start, NULL);
  //  just call the qsort library
  //  replace qsort by your parallel sorting algorithm using pthread
  parallel_sort(thread_no, size);
  // measure the end time
  gettimeofday(&end, NULL);

  if (!checking(intarr, size))
  {
    printf("The array is not in sorted order!!\n");
  }

  printf("Total elapsed time: %.4f s\n", (end.tv_sec - start.tv_sec) * 1.0 + (end.tv_usec - start.tv_usec) / 1000000.0);

  free(intarr);
  return 0;
}

int compare(const void *a, const void *b)
{
  return (*(unsigned int *)a > *(unsigned int *)b) ? 1 : ((*(unsigned int *)a == *(unsigned int *)b) ? 0 : -1);
}

int checking(unsigned int *list, long size)
{
  long i;
  printf("First : %d\n", list[0]);
  printf("At 25%%: %d\n", list[size / 4]);
  printf("At 50%%: %d\n", list[size / 2]);
  printf("At 75%%: %d\n", list[3 * size / 4]);
  printf("Last  : %d\n", list[size - 1]);
  for (i = 0; i < size - 1; i++)
  {
    if (list[i] > list[i + 1])
    {
      return 0;
    }
  }
  return 1;
}