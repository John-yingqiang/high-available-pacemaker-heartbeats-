LIBS  = -lm
CFLAGS = -Wall
CC = gcc

# Compile all .c to separate programs.

SRCS=$(wildcard *.c)

PROGS = $(SRCS:.c=)

all: $(PROGS)

clean: 
	rm -f $(PROGS)

%: %.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)
