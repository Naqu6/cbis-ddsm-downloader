#!/usr/local/bin/python

import requests
import csv
from multiprocessing import Pool
import io
import zipfile
import os

api_endpoint = "https://services.cancerimagingarchive.net/services/v3/TCIA/query/getImage?SeriesInstanceUID="
train_path = "../mass_case_description_train_set.csv"
test_path = "../mass_case_description_test_set.csv"

train_output_dir = "../train_data/"
test_output_dir = "../test_data/"

limit = 10
pool_workers = 10


def load_csv(csv_path):
	data = []
	key_to_index = {}

	with open(csv_path, "r") as f:
		reader = csv.reader(f)

		first_row = next(iter(reader))

		for index, key in enumerate(first_row):
			key_to_index[key] = index

		for row in reader:
			data.append(row)

	return data, key_to_index

def download_file(file_path, output_dir):
	seriesInstanceUID = file_path.split("/")[-2]

	response = requests.get(api_endpoint + seriesInstanceUID)

	buf = io.BytesIO(response.content)
	files = zipfile.ZipFile(buf)

	smallest_dcm_file = None

	for file in files.infolist():
		if ".dcm" == file.filename[-4:] and (smallest_dcm_file is None or smallest_dcm_file.file_size > file.file_size):
			smallest_dcm_file = file

	if smallest_dcm_file:
		return files.extract(smallest_dcm_file, output_dir)
	else:
		return None

def save_image(id_, label, file_path, output_dir):
	file_path = download_file(file_path, output_dir)

	if not file_path:
		return False

	os.system(f"mogrify -format png {file_path}")
	os.system(f"mv {file_path[:-4]}.png {output_dir}{label}_{id_}.png")
	os.system(f"rm {file_path}")

	print(f"Successfully downloaded {id_}")

	return True

def download_data(item):
	if save_image(*item):
		return True
	return False

def build_dataset(data, key_to_index, output_dir):
	pool = Pool(processes=pool_workers)

	download_meta_data = []
	counts = {}

	for item in data[:limit]:
		patient_id = item[key_to_index["patient_id"]]
		file_path = item[key_to_index["cropped image file path"]]

		if patient_id not in counts:
			counts[patient_id] = 0
		else:
			counts[patient_id] += 1

		download_meta_data.append((patient_id, item[key_to_index["pathology"]], file_path, output_dir))

	pool.map(download_data, download_meta_data)

def main():
	train_data, key_to_index = load_csv(train_path)
	build_dataset(train_data, key_to_index, train_output_dir)


if __name__ == "__main__":
	main()