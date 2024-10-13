"""
- Script load zst-files from reddit data dumps as txt files (https://files.pushshift.io/reddit/)
- each month has its own zst file
- 2 folders in current working directory: submissions and comments. In the folders lie the respective .zst files
- one txt files is created for each month, each subreddit and whether post is a submission or a comment 
"""

# libraries
import zstandard
import os
import json
from datetime import datetime
import logging.handlers

# set up logging
log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]

		reader.close()


if __name__ == "__main__":
	cwd=os.getcwd()
	data_folder= "comments" # folder with .rst files; switch to "submissions" if you want to read in submissions
	file_name= "RC_2019-12.zst" # switch to correct file name (Month and comment vs submission)
	file_path= os.path.join(cwd, data_folder, file_name)

	file_size = os.stat(file_path).st_size
	file_lines = 0
	file_bytes_processed = 0
	created = None
	bad_lines = 0

	new_folder = "filtered text files"
	os.makedirs(os.path.join(cwd, new_folder), exist_ok=True)  # Create the new folder if it doesn't exist
	
	for line, file_bytes_processed in read_lines_zst(file_path):
		try:
			# filtered data is stored in txt format, manually name it
			new_txt="abcd.txt" 
			file_path_new_txt=os.path.join(cwd,new_folder, new_txt)
			obj = json.loads(line)
			created = datetime.utcfromtimestamp(int(obj['created_utc']))
			
			if obj['subreddit'] == "Mommit":
				with open(file_path_new_txt, "a", encoding="utf-8") as file:
					file.write(json.dumps(obj) + "\n") # add a key wrap after each dictionary
				
		except (KeyError, json.JSONDecodeError) as err:
			bad_lines += 1
		file_lines += 1
		if file_lines % 100000 == 0:
			log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")


	log.info(f"Complete : {file_lines:,} : {bad_lines:,}")
