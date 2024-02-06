#include <stddef.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>

#include "list.h"
#include "threadpool.h"

// struct aligned to the size of a cache line
// so structs are allocated on different lines.
struct thread {
        pthread_t self;
        size_t loc;
        pthread_mutex_t qlock;
        struct list /* <future> */ thread_queue;

        // so the thread knows where it is
        struct thread_pool *tp;
} __attribute__ ((aligned (64)));

struct thread_pool {
        size_t max_threads;
        pthread_mutex_t plock;
        pthread_cond_t new_task;
        struct list /* <future> */ global_queue;
        struct thread* threads;
        bool shutdown;

        struct thread *dummy;
};

enum State {
        NOT_STARTED,
        IN_PROGRESS,
        FINISHED,
};

struct future {
        struct list_elem elem;
        enum State state;
        fork_join_task_t task;
        void * data;
        void * result;
        sem_t done;
        struct thread *owner;
        struct thread_pool *tp;
};

static _Thread_local struct thread *current_thread;


/**
 * Input:
 *      self: the thread doing the work
 *      fut: the future to execute
 *  Note that the future is currently part of no queue
 */
static void do_work(struct thread * self, struct future * fut) {
        pthread_mutex_lock(&self->qlock);
        fut->state = IN_PROGRESS;
        fut->owner = self;
        pthread_mutex_unlock(&self->qlock);

        fut->result = fut->task(self->tp, fut->data);
        
        pthread_mutex_lock(&self->qlock);
        fut->state = FINISHED;
        sem_post(&fut->done);
        pthread_mutex_unlock(&self->qlock);
}

static struct future * steal_work(struct thread * self) {
        struct thread_pool * tp = self->tp;
        for (size_t i = self->loc; i < tp->max_threads; i++) {
                struct thread * th = &tp->threads[i];
                pthread_mutex_lock(&th->qlock);
                if (!list_empty(&th->thread_queue)) {
                        struct future * work = list_entry(list_pop_back(&th->thread_queue),
                                        struct future, elem);
                        pthread_mutex_unlock(&th->qlock);
                        return work;
                }
                pthread_mutex_unlock(&th->qlock);
        }
        for (size_t i = 0; i < self->loc; i++) {
                struct thread * th = &tp->threads[i];
                pthread_mutex_lock(&th->qlock);
                if (!list_empty(&th->thread_queue)) {
                        struct future * work = list_entry(list_pop_back(&th->thread_queue),
                                        struct future, elem);
                        pthread_mutex_unlock(&th->qlock);
                        return work;
                }
                pthread_mutex_unlock(&th->qlock);
        }
        return NULL;
}

static void * worker_thread(void *arg) {
        // Do we need to check the state of potential work?
        // Are there some states that stealing doesn't make sense in?
        struct thread *self = (struct thread *)arg;
        struct thread_pool *tp = self->tp;
        current_thread = self;

        while (true) {
                struct future * fut = NULL;
                pthread_mutex_lock(&self->qlock);
                // First, we check our queue.
                if (!list_empty(&self->thread_queue)) {
                        fut = list_entry(list_pop_front(&self->thread_queue),
                                        struct future, elem);
                        pthread_mutex_unlock(&self->qlock);
                        do_work(self, fut);
                        continue;
                }
                pthread_mutex_unlock(&self->qlock);
                // Second, we check the global queue.
                pthread_mutex_lock(&tp->plock);
                if (!list_empty(&tp->global_queue)) {
                        fut = list_entry(list_pop_front(&tp->global_queue),
                                        struct future, elem);
                        pthread_mutex_unlock(&tp->plock);
                        do_work(self, fut);
                        continue;
                }
                pthread_mutex_unlock(&tp->plock);
                
                // Third, we steal from others.
                if ((fut = steal_work(self)) != NULL) {
                        do_work(self, fut);
                        continue;
                }

                // Finally, we wait for more work to be added to the global queue.
                pthread_mutex_lock(&tp->plock);
                if (tp->shutdown) {
                        pthread_mutex_unlock(&tp->plock);
                        return NULL;
                }
                // Need to check if work has been added
                // since the last time we checked.
                if (list_empty(&tp->global_queue)) {
                        if (tp->shutdown) {
                                pthread_mutex_unlock(&tp->plock);
                                return NULL;
                        }
                        pthread_cond_wait(&tp->new_task, &tp->plock);
                        
                        // If more threads were woken than new jobs exist,
                        // go back to the top and try to steal work.
                        if (list_empty(&tp->global_queue)) {
                                pthread_mutex_unlock(&tp->plock);
                                continue;
                        }
                }
                fut = list_entry(list_pop_front(&tp->global_queue), struct future, elem);
                pthread_mutex_unlock(&tp->plock);

                do_work(self, fut);
        }
}

struct thread_pool * thread_pool_new(int nthreads) {
        if (nthreads < 0) return NULL;
        
        struct thread_pool *tp = malloc(sizeof(struct thread_pool));
        tp->max_threads = nthreads;
        pthread_mutex_init(&tp->plock, NULL);
        pthread_cond_init(&tp->new_task, NULL);
        list_init(&tp->global_queue);
        tp->threads = aligned_alloc(64, sizeof(struct thread) * nthreads);
        tp->shutdown = false;

        pthread_mutex_lock(&tp->plock);
        for (size_t i = 0; i < tp->max_threads; i++) {
                struct thread *th = &tp->threads[i];
                pthread_mutex_init(&th->qlock, NULL);
                list_init(&th->thread_queue);
                th->tp = tp;
                th->loc = i;

                pthread_create(&th->self, NULL, worker_thread, th);
        } 

        // Create a dummy thread struct with global data in case
        // this thread needs do work when outside code calls future_get.
        struct thread *self = aligned_alloc(64, sizeof(struct thread));
        //self->qlock = tp->plock;
        self->thread_queue = tp->global_queue;
        self->tp = tp;
        tp->dummy = self;
        current_thread = NULL;

        pthread_mutex_unlock(&tp->plock);

        return tp;
}


void thread_pool_shutdown_and_destroy(struct thread_pool * tp) {
        pthread_mutex_lock(&tp->plock);
        tp->shutdown = true;
        pthread_cond_broadcast(&tp->new_task);
        pthread_mutex_unlock(&tp->plock);

        for (size_t i = 0; i < tp->max_threads; i++) {
               struct thread* th = &tp->threads[i];
               pthread_join(th->self, NULL);
        }

        free(current_thread); 
        free(tp->threads);
        free(tp->dummy);
        free(tp);
}


struct future * thread_pool_submit(
                struct thread_pool *pool,
                fork_join_task_t task,
                void * data) {
        struct future *fut = malloc(sizeof(struct future));
        fut->state = NOT_STARTED;
        fut->task = task;
        fut->data = data;
        // This will assign owner to NULL if it is in the global queue.
        fut->owner = current_thread;
        fut->tp = pool;
        sem_init(&fut->done, 0, 0);

        if (current_thread) {
                pthread_mutex_lock(&current_thread->qlock);
                list_push_back(&current_thread->thread_queue, &fut->elem);
                pthread_cond_signal(&pool->new_task);
                pthread_mutex_unlock(&current_thread->qlock);
        } else {
                pthread_mutex_lock(&pool->plock);
                list_push_back(&pool->global_queue, &fut->elem);
                pthread_cond_signal(&pool->new_task);
                pthread_mutex_unlock(&pool->plock);
        }
        
        return fut;
}

void * future_get(struct future * fut) {
        if (fut->state == NOT_STARTED) {
                struct thread_pool *tp = fut->tp;
                struct thread *self;

                if (current_thread) {
                        self = current_thread;
                } else {
                        self = tp->dummy;
                }

                if (fut->owner == NULL) {
                        pthread_mutex_lock(&tp->plock);
                        struct list_elem *e;
                        for (e = list_begin(&tp->global_queue);
                                e != list_end(&tp->global_queue); e = list_next (e)) {
                                if (e == &fut->elem && fut->state == NOT_STARTED) {
                                        list_remove(&fut->elem);
                                        fut->owner = current_thread;
                                        pthread_mutex_unlock(&tp->plock);
                                        do_work(self, fut);
                                        pthread_mutex_lock(&tp->plock);
                                        break;
                                         
                                }
                        }
                        pthread_mutex_unlock(&tp->plock);
                } else {
                        struct thread *th = fut->owner;
                        pthread_mutex_lock(&th->qlock);
                        struct list_elem *e;
                        for (e = list_begin(&th->thread_queue);
                                        e != list_end(&th->thread_queue); e = list_next(e)) {
                                if (e == &fut->elem && fut->state == NOT_STARTED) {
                                        list_remove(&fut->elem);
                                        fut->owner = current_thread;
                                        pthread_mutex_unlock(&th->qlock);
                                        do_work(self, fut);
                                        pthread_mutex_lock(&th->qlock);
                                        break;
                                         
                                }
                        }
                        pthread_mutex_unlock(&th->qlock);
                }
        }
        while (fut->state != FINISHED) {
                sem_wait(&fut->done);
        }
        return fut->result;

}

void future_free(struct future * fut) {
        free(fut);
}
