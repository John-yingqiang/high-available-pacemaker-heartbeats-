#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <strings.h>
#include <assert.h>


/* Fast 64bit pseudo random number generator. */
uint64_t rand_seed=8; /* The initial state must be seeded with a nonzero value. */
uint64_t xorshift64star(void) {
	rand_seed ^= rand_seed >> 12; // a
	rand_seed ^= rand_seed << 25; // b
	rand_seed ^= rand_seed >> 27; // c
	return rand_seed * UINT64_C(2685821657736338717);
}

/* calc time difference. */
int
timeval_subtract (struct timeval *result, struct timeval *x, struct timeval *y)
{
  /* Perform the carry for the later subtraction by updating y. */
  if (x->tv_usec < y->tv_usec) {
    int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
    y->tv_usec -= 1000000 * nsec;
    y->tv_sec += nsec;
  }
  if (x->tv_usec - y->tv_usec > 1000000) {
    int nsec = (x->tv_usec - y->tv_usec) / 1000000;
    y->tv_usec += 1000000 * nsec;
    y->tv_sec -= nsec;
  }

  /* Compute the time remaining to wait.
     tv_usec is certainly positive. */
  result->tv_sec = x->tv_sec - y->tv_sec;
  result->tv_usec = x->tv_usec - y->tv_usec;

  /* Return 1 if result is negative. */
  return x->tv_sec < y->tv_sec;
}

void usage(char *prog_name)
{
    fprintf(stderr, "Usage: %s <-o output_file_name> <-s file_size_in_GB> [-S] [-r random_seed]\n", prog_name);
    fprintf(stderr, "\t By default, %s will use current time as random_seed.\n", prog_name);
    fprintf(stderr, "\t random_seed must be nonzero value.\n");
    fprintf(stderr, "\t -a: async write mode, default is sync write mode.\n");
    fprintf(stderr, "\t -b: IO unit size in KiB, default 64KiB.\n");
    fprintf(stderr, "\t To test duplicate write, set random_seed to same non-zero value for different %s processes\n", prog_name);
}

int main(int argc, char *argv[])
{

    const uint64_t blk_sz = 1024;
    uint64_t blk_per_buf = 64;
    uint64_t buf_sz = blk_sz * blk_per_buf;
    char *buf = NULL;
    const int64_t io_time_limit = 5;

    int fd, rc;
    uint64_t i = 0, j = 0, total_bufs = 0, prev_bufs = 0, n_buf_written = 0;
    struct timeval t_start, t_prev, t_io_start, t_io_end, t_io_duration, t_now, t_elapsed, t_total;
    double elapsed_sec, total_sec, io_sec, bandwidth_now = 0.0, bandwidth_avg = 0.0;
    double offset_mb;
    int rseed_set = 0;
    uint64_t randnum;

    int opt;
    int fflag = O_CREAT|O_RDWR|O_SYNC;
    char *fname = NULL;
    uint64_t fsz = 0;


    if (argc < 3) {
        usage(argv[0]);
        exit(1);
    }

    while ((opt = getopt(argc, argv, "o:s:r:ab:")) != -1) {
        switch (opt) {
        case 'o':
            fname = optarg;
            break;
        case 's':
            fsz = 1024 * 1024 * 1024 * atol(optarg);
            break;
        case 'a':
            fflag &= ~O_SYNC;
            break;
        case 'b':
            blk_per_buf = atol(optarg);
            buf_sz = blk_per_buf * blk_sz;
            break;
        case 'r':
            rand_seed = atol(optarg);
            if (rand_seed == 0) {
                usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            rseed_set = 1;
            break;
        default: /* '?' */
            usage(argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    if (fname == NULL || fsz == 0) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    buf = (char *)malloc(buf_sz);
    assert(buf != NULL);

    total_bufs = fsz / buf_sz;

    /* Get random seed. */
    if (rseed_set == 0) {
        gettimeofday(&t_now, NULL);
        rand_seed = t_now.tv_sec;
    }
    assert(rand_seed != 0);

    printf("fsz: %lu, buf_sz: %lu, total_bufs: %lu, seed:%lu\n", fsz, buf_sz, total_bufs, rand_seed);

	fd = open(fname, fflag, S_IRUSR|S_IWUSR);
	if (fd < 0) {
        perror("Open output file failed!\n");
		return 1;
	}

    gettimeofday(&t_start, NULL);
    gettimeofday(&t_prev, NULL);

    /* Main write loop. */
    bzero(buf, buf_sz);
	for(i = 0; i < total_bufs; i++) {
        /* Generate random data, every 1k block is made unique. */
        for(j = 0; j < blk_per_buf; j++) {
            uint64_t *p = (uint64_t *)&(buf[j * blk_sz]);
		    randnum = xorshift64star();
            //printf("%lx\n", randnum);
		    *p = randnum;
        }

        gettimeofday(&t_io_start, NULL);

        /* Write data out. */
		rc = write(fd, buf, buf_sz);
        if(rc < 0) {
            perror("Write Error!\n");
        }

        /* Latency measurement. */
        gettimeofday(&t_io_end, NULL);
        timeval_subtract(&t_io_duration, &t_io_end, &t_io_start);
        io_sec = t_io_duration.tv_sec + t_io_duration.tv_usec / 1000000.0;
        if (t_io_duration.tv_sec > io_time_limit) {
            printf("-------------------WARNING: IO HANG TIME GAP-------------------: %f seconds\n", io_sec);
        }

        /* Bandwidth measurement. */
        gettimeofday(&t_now, NULL);
        timeval_subtract(&t_elapsed, &t_now, &t_prev);
        timeval_subtract(&t_total, &t_now, &t_start);
        elapsed_sec = t_elapsed.tv_sec + t_elapsed.tv_usec / 1000000.0;
        total_sec = t_total.tv_sec + t_total.tv_usec / 1000000.0;
        if (elapsed_sec > 1.0) {
            n_buf_written = i - prev_bufs;
            prev_bufs = i;
            bandwidth_now = n_buf_written * buf_sz / (1024*1024) / elapsed_sec;
            offset_mb = i * buf_sz * 1.0 / (1024*1024);
            bandwidth_avg= offset_mb / total_sec;
            printf("%lds: Written: %9.1fMB, avg Bandwidth: %8.1fMB, curr Bandwidth: %8.2fMB", t_now.tv_sec, offset_mb, bandwidth_avg, bandwidth_now);
            if (elapsed_sec > io_time_limit) {
                printf(", time gap: %f, n_buf:%lu\n", elapsed_sec, n_buf_written);
            } else {
                printf("\n");
            }
            fflush(stdout);
            gettimeofday(&t_prev, NULL);
        }
	}

	close(fd);

    bandwidth_avg= i * buf_sz / (1024*1024) / total_sec;
    printf("IO Done: Total data written: %lu, Avg Bandwidth: %7.1fMB.\n", i * buf_sz, bandwidth_avg);

    return 0;
}
