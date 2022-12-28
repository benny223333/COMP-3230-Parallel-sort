# COMP-3230-Parallel-sort
A parallel sort algorithm, utlizes multi threading to increase the speed of sorting
# Usage
1. Open Terminal in Linux
2. Change the working directory to where the qsort.c file is
3. Compile the file using the command: gcc -pthread qsort.c -o qsort
4. To run the compiled file, use the commnand: ./qsort <number> [<no_of_workers>], where <number> specifies how many numbers to be sorted (must fill in) and [<no_of_workers>] specifies how many worker threads to handle the numbers (optional, default to 4, cannot be lower than 1)
