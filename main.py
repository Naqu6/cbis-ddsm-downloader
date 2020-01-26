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

output_dir = "../data/"
limit = 100
pool_workers = 10


def load_csv(csv_path):
	data = {}
	index_to_key = []

	with open(train_path, "r") as f:
		reader = csv.reader(f)

		first_row = next(iter(reader))

		for key in first_row:
			data[key] = []
			index_to_key.append(key)

		for row in reader:
			for i, item in enumerate(row):
				data[index_to_key[i]].append(item)

	return data

def download_file(file_path):
	seriesInstanceUID = file_path.split("/")[-2]

	response = requests.get(api_endpoint + seriesInstanceUID)

	buf = io.BytesIO(response.content)
	files = zipfile.ZipFile(buf)

	for file_name in files.namelist():
		if ".dcm" == file_name[-4:]:
			return files.extract(file_name, output_dir)

def save_image(id_, file_path):
	file_path = download_file(file_path)

	if not file_path:
		return False

	os.system(f"mogrify -format png {file_path}")
	os.system(f"mv {file_path[:-4]}.png {output_dir}{id_}.png")
	os.system(f"rm {file_path}")

	print(f"Successfully downloaded {id_}")

	return True



def build_dataset(data):
	pool = Pool(processes=pool_workers)

	indexes = [i for i in range(len(next(iter(data.values()))))]
	counts = {}

	def download_data(index):
		patient_id = data["patient_id"][index]

		if patient_id not in counts:
			counts[patient_id] = 0
		else:
			counts[patient_id] += 1

		patient_id += "_" + str(counts[patient_id])

		if save_image(patient_id, data["image file path"][index]):
			return True
		return False

	for i in range(10):
		download_data(i)

def main():
	train_data = load_csv(train_path)
	build_dataset(train_data)


if __name__ == "__main__":
	main()