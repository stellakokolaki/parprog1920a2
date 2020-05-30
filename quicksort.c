/*
Multithreading programming
Quicksort with threads - implemented with c language
compile with e.g. gcc -O2 -Wall -pthread quicksort.c -o quicksort -DTHREADS=4
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>

#define N 10000000
#define CUTOFF 100
#define QUEUE_SIZE N

// conditional variable, signals array put operation (receiver waits on this)
pthread_cond_t msg_in = PTHREAD_COND_INITIALIZER;
// conditional variable, signals array get operation (sender waits on this)
pthread_cond_t msg_out = PTHREAD_COND_INITIALIZER;
// mutex protecting common resources
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;


// Thread availability status of queue.
struct Msg_params {
    double *begin_pos_msg;
    int size;
    int end_msg;
    int end_pthread;
};
struct Msg_params mParam[QUEUE_SIZE];


// Simple sorting algorithm so sort the remaining elements in the array
void inssort(double *array,int n) {
    int i,j;
    double t;
    for (i = 1; i < n; i++) {
        j = i;
        while ((j > 0) && (array[j - 1] > array[j])) {
            t = array[j - 1];  array[j - 1] = array[j];
            array[j] = t;
            j--;
        }
    }
}

// The algorithm to partition the array
int partition(double *array, int n) {
    int first, last, middle;
    double t, p;
    int i, j;

    // take first, last and middle positions
    first = 0;
    middle = n - 1;
    last = n / 2;

    // put median-of-3 in the middle
    if (array[middle] < array[first]) {
        t = array[middle];
        array[middle] = array[first];
        array[first] = t;
    }
    if (array[last] < array[middle]) {
        t = array[last];
        array[last] = array[middle];
        array[middle] = t;
    }
    if (array[middle] < array[first]) {
        t = array[middle];
        array[middle] = array[first];
        array[first] = t;
    }

    // partition (first and last are already in correct half)
    p = array[middle]; // pivot
    for (i = 1, j = n - 2;; i++, j--) {
        while (array[i] < p) {
            i++;
        }
        while (p < array[j]) {
            j--;
        }
        if (i >= j) {
            break;
        }

        t = array[i];
        array[i] = array[j];
        array[j] = t;
    }
    // return position of pivot
    return i;
}

// FIFO queue. First in, first out.
int queue_capacity = 0, q_first_in = 0, q_first_out = -1;
// Process in order to send messages to the queue.
void send_msg(double *begin_pos_msg, int size, int end_msg, int end_pthread) {
    struct Msg_params queue_msg;

    while (queue_capacity == QUEUE_SIZE - 1) {
        pthread_cond_wait(&msg_out, &mutex);
    }

    queue_msg.begin_pos_msg = begin_pos_msg;
    queue_msg.size = size;
    queue_msg.end_msg = end_msg;
    queue_msg.end_pthread = end_pthread;

    if(q_first_out == QUEUE_SIZE - 1) {
        q_first_out = -1;
    }

    mParam[++q_first_out] = queue_msg;

    queue_capacity++;

    pthread_cond_signal(&msg_in);
    pthread_mutex_unlock(&mutex);
}

// Send messages to the queue
struct Msg_params receive_Msg() {
    struct Msg_params send_msg_to_the_queue;

    while (queue_capacity == 0) {
        pthread_cond_wait(&msg_in, &mutex);
    }
    send_msg_to_the_queue = mParam[q_first_in++];

    if(q_first_in == QUEUE_SIZE) {
        q_first_in = 0;
    }
    return send_msg_to_the_queue;
}

// Policy that every thread will follow
void *thread_Proc(void *args) {
    struct Msg_params send_msg_to_the_queue;
    int i;

    while ( N < 10000000) {
        // Retreive array message
        send_msg_to_the_queue = receive_Msg();

        // The message was array end_pthread, so, gracefully stop and exit
        if (send_msg_to_the_queue.end_pthread == 1) {
            break;
        }
        // Check if the message needs to be checked, either for partitioning or for sorting
        if (send_msg_to_the_queue.end_msg != 1) {

            // The message is ready to be sorted, proceed and signal
            if (send_msg_to_the_queue.size <= CUTOFF) {
                inssort(send_msg_to_the_queue.begin_pos_msg, send_msg_to_the_queue.size);
                send_msg(send_msg_to_the_queue.begin_pos_msg, send_msg_to_the_queue.size, 1, 0);
            }
            // The message must me partitioned, proceed and signal for each slice
            else {
                i = partition(send_msg_to_the_queue.begin_pos_msg, send_msg_to_the_queue.size);
                send_msg(send_msg_to_the_queue.begin_pos_msg, i, 0, 0);
                send_msg(send_msg_to_the_queue.begin_pos_msg + i, send_msg_to_the_queue.size - i, 0, 0);
            }
        }
        // we just pass by the message to the queue again because
        // it was array end_msgmessage
        else {
            send_msg(
                send_msg_to_the_queue.begin_pos_msg,
                send_msg_to_the_queue.size,
                send_msg_to_the_queue.end_msg,
                send_msg_to_the_queue.end_pthread
            );
        }
    }
    pthread_exit(NULL);
}





int main() {
    struct Msg_params send_msg_to_the_queue;
    pthread_t threadIds[THREADS];
    double *array;
    array = (double *)malloc(N * sizeof(double));
    int i,k;

    // Fill the array with numbers for quicksorting & Let the flag-thread messages travel
    srand(time(NULL));
    for (i = 0; i < N; i++) {
        array[i] = (double)rand() / RAND_MAX;
    }
    for (i = 0; i < THREADS; i++) {
        if (Msg_params(&threadIds[i], NULL, thread_Proc, NULL) != 0) {
            exit(1);
        }
    }
    k = 0;
    while( N < 10000000) {
        if (k == N) {
            for (i = 0; i < THREADS; i++) {
                send_msg(
                    send_msg_to_the_queue.begin_pos_msg,
                    send_msg_to_the_queue.size,
                    send_msg_to_the_queue.end_msg,1
                );
            }
            break;
        }

        send_msg_to_the_queue = receive_Msg();

        // If the part of the array is sorted, collect and sort another.
        if (send_msg_to_the_queue.end_msg == 1) {
            k += send_msg_to_the_queue.size;
        }
        // Else, finish the partition
        else {
            send_msg(
                send_msg_to_the_queue.begin_pos_msg,
                send_msg_to_the_queue.size,
                send_msg_to_the_queue.end_msg,
                send_msg_to_the_queue.end_pthread
            );
        }
    }


    // Join the threads since the processing is done
    for(i = 0; i < THREADS; i++) {
        pthread_join(threadIds[i], NULL);// this is blocking
    }

    // Check if the sorting was operated successfuly
     for (i=0;i<(N-1);i++)
     {
         if (array[i] > array[i+1])
         {
             printf("Sort did not end successfuly!\n");
             break;
         }
     }
    // Empty the queue
    while (queue_capacity > 0) {
        receive_Msg();
    }

    // Free mutexes
    free(array);
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&msg_in);
    pthread_cond_destroy(&msg_out);
    return 0;
}
