from pymongo import MongoClient
import csv
import datetime

url = "mongodb://localhost:27017"
collection = MongoClient(url)

# create new database
db = collection.db1

with open('logs.txt', 'w') as logs_file:
    batch_size = 1000
    file_names = ["Odata2019File.csv", "Odata2020File.csv"]
    years = [2019, 2020]

    for j in range(2):
        file_name, year = file_names[j], years[j]
        with open(file_name, "r", encoding="cp1251") as csv_file:

            start_time = datetime.datetime.now()
            print("Reading file " + f"{file_name}")
            print("--------------------------\n")
            logs_file.write(str(start_time) + ", " + file_name + 'n')
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            i = 0
            batches_num = 0
            document_bundle = []


            inserted_values = db.number_of_inserted_values.find_one({"year": year})
            if inserted_values == None:
               inserted_values = 0
            else:
                inserted_values = inserted_values["number_of_inserted_values"]
                print("Also " + f"{inserted_values}" + " rows was inserted")
                print("--------------------------\n")

            for row in csv_reader:

                if batches_num * batch_size + i < inserted_values:
                    i += 1
                    if i == batch_size:
                        i = 0
                        batches_num += 1
                    continue

                document = row
                document['year'] = year
                document_bundle.append(document)
                i += 1

                if i == batch_size:
                    i = 0
                    batches_num += 1
                    db.collection_zno_result_table.insert_many(document_bundle)
                    document_bundle = []

                    if batches_num == 1:
                        db.number_of_inserted_values.insert_one({"number_of_inserted_values": batch_size, "year": year})
                    else:
                        db.number_of_inserted_values.update_one({
                            "year": year, "number_of_inserted_values": (batches_num - 1) * batch_size},
                            {"$inc": {
                                "number_of_inserted_values": batch_size
                            }})

            if i != 0 and document_bundle:
                db.number_of_inserted_values.update_one({
                    "year": year, "number_of_inserted_values": batches_num * batch_size},
                    {"$inc": {
                        "number_of_inserted_values": i
                    }})
                db.collection_zno_result_table.insert_many(document_bundle)

            end_time = datetime.datetime.now()
            logs_file.write(str(end_time) + ", " + file_name + ' end\n')
            print('time:', end_time - start_time)
            print("--------------------------\n")



with open("result.csv", "w", encoding="utf-8") as file:
     writer = csv.writer(file)
     result = db.collection_zno_result_table.aggregate([

          {"$match": {"mathTestStatus": "Зараховано"}},

          {"$group": {
            "_id": {
                "year": "$year",
                "regname": "$REGNAME"

            },
             "minball": {
                 "$min": "$mathBall100"
            }
          }},
           {"$sort": {"_id": 1}}
     ])
     writer.writerow(["Область", "Рік", "Мінімальний бал з математики"])
     for doc in result:
         year = doc["_id"]["year"]
         regname = doc["_id"]["regname"]
         minball = doc["minball"]
         writer.writerow([regname, year, minball])