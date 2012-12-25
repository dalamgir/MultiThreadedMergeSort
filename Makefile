CC = gcc
CFLAGS = -g
FILE = sort_thread.c

all:
	make clean
	${CC} ${CFLAGS} ${FILE} -o sort_thread

clean:
	rm -f sort_thread
