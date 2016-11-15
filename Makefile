
all: rio

rio: rio.c
	cc -O2 -D_FILE_OFFSET_BITS=64 -o rio rio.c -Wall -W -pthread

clean:
	rm -f rio

.PHONY: all clean
