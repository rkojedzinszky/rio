#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <sys/time.h>

static uint32_t timeout = 0;

#define NRAND48_MAX (1ULL << 31)

static const char *file = NULL;
static unsigned bshift = 12;
static uint32_t blocksize;
static unsigned long long blocks;
static uint64_t blocks_per_thread;
static int loop = 1;
static int nthread = 1;

struct thread_info;

struct thread_info {
	pthread_t thread;
	struct thread_info *next;
	pthread_mutex_t io_done_mutex;
	int fd;
	char *buf;
	uint32_t io_done;
	uint64_t block_start;
	uint64_t block_end;
	unsigned short xseed[3];
};

static struct thread_info* thread_alloc()
{
        int fd;
	struct thread_info *thread = malloc(sizeof(struct thread_info));

	thread->next = NULL;
	pthread_mutex_init(&thread->io_done_mutex, NULL);
	thread->io_done = 0;

	thread->buf = malloc(blocksize);

	fd = open("/dev/urandom", O_RDONLY);
	read(fd, thread->buf, blocksize);
	close(fd);

	return thread;
}

static void thread_free(struct thread_info *thread)
{
	free(thread->buf);

	pthread_mutex_destroy(&thread->io_done_mutex);

	free(thread);
}

static inline void io_done_inc(struct thread_info *thread)
{
	pthread_mutex_lock(&thread->io_done_mutex);
	++thread->io_done;
	pthread_mutex_unlock(&thread->io_done_mutex);
}

static inline uint32_t io_done_get(struct thread_info *thread)
{
	uint32_t ret;

	pthread_mutex_lock(&thread->io_done_mutex);
	ret = thread->io_done;
	pthread_mutex_unlock(&thread->io_done_mutex);

	return ret;
}

static uint32_t io_done_get_all(struct thread_info *thread)
{
	uint32_t ret = 0;

	while (thread != NULL) {
		ret += io_done_get(thread);
		thread = thread->next;
	}

	return ret;
}

struct op_t {
	const char *name;
	void (*open)(struct thread_info *thread);
	int (*op)(struct thread_info *thread);
	void (*close)(struct thread_info *thread);
};

static void common_close(struct thread_info *thread)
{
	close(thread->fd);
	thread->fd = -1;
}

// sequential read
static void sread_open(struct thread_info *thread)
{
	thread->fd = open(file, O_RDONLY);

	lseek(thread->fd, thread->block_start * blocksize, SEEK_SET);
}

static int sread_op(struct thread_info *thread)
{
	if (read(thread->fd, thread->buf, blocksize) != blocksize) {
		return 0;
	}

	if ((uint64_t)lseek(thread->fd, 0, SEEK_CUR) >= thread->block_end * blocksize) {
		// reset file pointer to beginning of region
		lseek(thread->fd, thread->block_start * blocksize, SEEK_SET);
	}

	return 1;
}

static struct op_t sread_ops = {
	.name = "seq read",
	.open = sread_open,
	.op = sread_op,
	.close = common_close
};

static void swrite_open(struct thread_info *thread)
{
	thread->fd = open(file, O_WRONLY);

	lseek(thread->fd, thread->block_start * blocksize, SEEK_SET);
}

static int swrite_op(struct thread_info *thread)
{
	if (write(thread->fd, thread->buf, blocksize) != blocksize) {
		return 0;
	}

	(void) fsync(thread->fd);

	if ((uint64_t)lseek(thread->fd, 0, SEEK_CUR) >= thread->block_end * blocksize) {
		// reset file pointer to beginning of region
		lseek(thread->fd, thread->block_start * blocksize, SEEK_SET);
	}

	return 1;
}

static struct op_t swrite_ops = {
	.name = "seq write",
	.open = swrite_open,
	.op = swrite_op,
	.close = common_close
};

static void rcommon_open(struct thread_info *thread)
{
	int fd = open("/dev/random", O_RDONLY);
	read(fd, thread->xseed, sizeof(thread->xseed));
	close(fd);
}

static void rcommon_seek(struct thread_info *thread)
{
	uint64_t block = thread->block_start + nrand48(thread->xseed) * blocks_per_thread / NRAND48_MAX;

	lseek(thread->fd, block * blocksize, SEEK_SET);
}

static void rread_open(struct thread_info *thread)
{
	thread->fd = open(file, O_RDONLY);

	rcommon_open(thread);
}

static int rread_op(struct thread_info *thread)
{
	rcommon_seek(thread);

	return read(thread->fd, thread->buf, blocksize) == blocksize;
}

static struct op_t rread_ops = {
	.name = "random read",
	.open = rread_open,
	.op = rread_op,
	.close = common_close
};

static void rwrite_open(struct thread_info *thread)
{
	thread->fd = open(file, O_WRONLY);

	rcommon_open(thread);
}

static int rwrite_op(struct thread_info *thread)
{
	rcommon_seek(thread);

	if (write(thread->fd, thread->buf, blocksize) != blocksize) {
		return 0;
	}

	(void) fsync(thread->fd);

	return 1;
}

static struct op_t rwrite_ops = {
	.name = "random write",
	.open = rwrite_open,
	.op = rwrite_op,
	.close = common_close
};

static struct op_t *op;

static void* iothread(struct thread_info *thread)
{
	op->open(thread);

	while (loop) {
		(void) op->op(thread);

		io_done_inc(thread);
	}

	op->close(thread);

	return NULL;
}

static void help()
{
	puts("Usage: rio [-n nthread] [-b block shift] [-t timeout in secs] [-r] [-w] <file>");
	puts(" -r : random mode");
	puts(" -w : write mode");
	puts("");
	exit(EXIT_FAILURE);
}

static struct thread_info* start_threads(int n)
{
	int i;

	struct thread_info *top = NULL;

	uint64_t block_start = 0;

	for (i = 0; i < n; ++i) {
		struct thread_info *cur = thread_alloc();
		cur->next = top;
		top = cur;

		cur->block_start = block_start;
		block_start += blocks_per_thread;
		cur->block_end = block_start;

		pthread_create(&cur->thread, NULL, (void* (*)(void*))iothread, cur);
	}

	return top;
}

static inline uint64_t tv_interval(const struct timeval *start, const struct timeval *end)
{
	return (end->tv_sec - start->tv_sec) * 1000000 + (end->tv_usec - start->tv_usec);
}

static void stats_loop(struct thread_info *thread)
{
	struct timeval start;
	uint64_t start_io;

	gettimeofday(&start, NULL);
	start_io = io_done_get_all(thread);

	if (timeout == 0) {
		struct timeval prev;
		uint64_t prev_io;

		prev = start;
		prev_io = start_io;

		while (1) {
			struct timeval next;
			uint64_t next_io;

			sleep(1);

			gettimeofday(&next, NULL);
			next_io = io_done_get_all(thread);

			printf("Stats: TOTAL io=%7llu Avg=%8.2f/s | LAST io=%7llu (%8.2f/s)\n",
					(unsigned long long)(next_io - start_io), (double)(next_io - start_io) * 1000000.0 / tv_interval(&start, &next),
					(unsigned long long)(next_io - prev_io), (double)(next_io - prev_io) * 1000000.0 / tv_interval(&prev, &next));

			prev_io = next_io;
			prev = next;
		}
	} else {
		struct timeval end;
		uint64_t end_io;

		sleep(timeout);

		gettimeofday(&end, NULL);
		end_io = io_done_get_all(thread);

		loop = 0;

		printf("{\"mode\":\"%s\",\"blocksize\":%u,\"total_blocks\":%llu,\"nthread\":%u,\"interval\":%f,\"io_done\":%llu,\"iops\":%f}\n",
				op->name,
				blocksize,
				blocks,
				nthread,
				(double)tv_interval(&start, &end) / 1000000.0,
				(unsigned long long)(end_io - start_io),
				(double)(end_io - start_io) * 1000000.0 / tv_interval(&start, &end));
	}
}

int main(int argc, char *argv[])
{
	int optchr;
	int fd;
	struct thread_info *threads;
	int write = 0;
	int random = 0;

	while ((optchr = getopt(argc, argv, "n:b:t:wr")) != -1) {
		switch(optchr) {
		case 'n':
			nthread = atoi(optarg);
			break;
		case 'b':
			bshift = atoi(optarg);
			break;
		case 't':
			timeout = atoi(optarg);
			break;
		case 'w':
			write = 1;
			break;
		case 'r':
			random = 1;
			break;
		default:
			help();
			break;
		}
	}

	if (optind >= argc) {
		help();
	}

	if (random == 0) {
		if (write == 0) {
			op = &sread_ops;
		} else {
			op = &swrite_ops;
		}
	} else {
		if (write == 0) {
			op = &rread_ops;
		} else {
			op = &rwrite_ops;
		}
	}

	blocksize = 1 << bshift;

	file = argv[optind];

	fd = open(file, O_RDONLY);
	if (-1 == fd) {
		perror("open");
		exit(EXIT_FAILURE);
	}

	blocks = lseek(fd, 0, SEEK_END) / blocksize;
	close(fd);

	blocks_per_thread = blocks / nthread;

	if (timeout == 0) {
		printf("Configuration:\n\tfile = %s\n"
				"\tmode = %s\n"
				"\tblocksize = %u\n\tblocks = %llu\n\tthreads = %d\n",
				file, op->name,
				blocksize, blocks, nthread);
	}

	threads = start_threads(nthread);

	stats_loop(threads);

	while (threads != NULL) {
		struct thread_info *n = threads->next;
		void *retval;

		(void) pthread_join(threads->thread, &retval);
		thread_free(threads);
		threads = n;
	}

	return 0;
}
