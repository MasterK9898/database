import subprocess
import time
import threading
import queue
import re
import sys

if(len(sys.argv) != 6):
    print("Usage: python3 test.py <folder_dir> <bin_dir> <table_sql_dir> <test_sql_dir> <output_dir>")
    exit()
  
folder_dir = sys.argv[1]
bin_dir = sys.argv[2]
table_sql_dir = sys.argv[3]
test_sql_dir = sys.argv[4]
output_dir = sys.argv[5]



# This function will read the output of the command and print it
def read_output(out_pipe):
    count = 1
    with open(output_dir, 'w') as file:
      output = iter(out_pipe.readline, '')
      for line in output:
        print(line, end='')
        if(re.match(r'(^[A-Z]+:)|VALID', line)):
          file.write(str(count) + '\n')
          file.write(line)
          file.write('\n')
          file.flush()
          count += 1

# This function will send input to the subprocess every 10 seconds
def write_input(in_queue, in_pipe):
    while True:
        time.sleep(1)  # Wait for 1 second for each test
        if not in_queue.empty():
            in_pipe.write(in_queue.get() + '\n')
            in_pipe.flush()  # Ensure it gets sent to the subprocess


# Remove the original directory
subprocess.run(['rm', '-r', folder_dir])

# Create a new one
subprocess.run(['mkdir', folder_dir])

# Read from the test files
with open(table_sql_dir, 'r') as file:
    table_content = file.read()

with open(test_sql_dir, 'r') as file:
    test_content = file.read()

test_list = re.split(r'(?:\(\d+\))', test_content)

# trime and filter the input
test_list = [s for s in (s.strip() for s in test_list) if s]

# Start the subprocess with pipes for stdin and stdout
process = subprocess.Popen([bin_dir, 'test', folder_dir], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, universal_newlines=True)

# Create a queue to send input to the subprocess
input_queue = queue.Queue()

# Start a thread to read the subprocess output
output_thread = threading.Thread(target=read_output, args=(process.stdout,))
output_thread.daemon = True
output_thread.start()

# Start a thread to write input to the subprocess
input_thread = threading.Thread(target=write_input, args=(input_queue, process.stdin))
input_thread.daemon = True
input_thread.start()

# Wait for DB to start
time.sleep(1)

i = -1

# Main program loop
while True:
    if(i >= len(test_list)):
        break
    if(i == -1): 
        input_queue.put(table_content)
    else:
        input_queue.put(test_list[i])
    i += 1

    time.sleep(1)  # Wait for 1 second1

# the subprocess will always printout new things, so we can just leave it for sometime
time.sleep(1)

print("Done")