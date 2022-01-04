import json, pprint, requests, textwrap, time, os
from concurrent.futures import ThreadPoolExecutor, ALL_COMPLETED, ProcessPoolExecutor
import concurrent.futures
import traceback


def main():
    host = 'http://localhost:9001'
    spark_master = 'local[*]'
    headers = {'Content-Type': 'application/json'}
    session_create_resp = create_spark_session(host, spark_master, headers)

    print("--------------------SESSION INFORMATION--------------------")
    print(session_create_resp.json())
    print("------------------------------------------------------------")

    session_url = host + session_create_resp.headers['location']
    session_metadata = requests.get(session_url, headers=headers)
    statements_url = session_url + '/statements'

    print("--------------------SLEEPING FOR 10s FOR SESSION CREATION--------------------")
    time.sleep(10)
    print("-----------------------------------------------------------------------------")

    total_epochs = 2
    epoch_id = 1
    total_tf_workers = 10
    base_raw_path = "/Users/apatnam/dev/sparktensor-e2e-data/raw-training-data"
    base_epoch_path = "/Users/apatnam/dev/sparktensor-e2e-data/epochs"

    try:
        while epoch_id <= total_epochs:
            path = base_epoch_path + "/" + str(epoch_id)
            code = {
                'code': textwrap.dedent("""
                    import spark.implicits._
                    import org.apache.spark.sql.types.IntegerType
                    import org.apache.spark.sql.DataFrame
                    import org.apache.spark.sql.SaveMode
                    import org.apache.spark.sql.types._
                    import java.util.concurrent.atomic.AtomicInteger
                    
                    val totalWorkersNotified = new AtomicInteger()
        
                    val df = {
                        spark
                            .read
                            .csv("%s")
                            .toDF("age", "first", "last", "company")
                            .withColumn("age",$"age".cast(IntegerType))
                            .repartition(%s)
                    }
                    df.write.mode(SaveMode.Overwrite).json("%s")
                """ % (base_raw_path, str(total_tf_workers), path)
                )
            }
            execute_spark(code, host, statements_url, headers)

            json_files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]
            executor = ProcessPoolExecutor(max_workers=10)
            tf_workers = range(0, 10)
            futures = [executor.submit(tf_worker_dummy_logic, tf_worker_id, json_files[tf_worker_id], total_tf_workers, epoch_id, host, headers, base_epoch_path, statements_url) for tf_worker_id in tf_workers]
            for future in concurrent.futures.as_completed(futures):
                try:
                    tf_logic_done = future.result()
                except Exception as err:
                    traceback.print_exc()
            epoch_id += 1
    except Exception as err:
        traceback.print_exc()
        # close_session(session_metadata, host, headers)
    finally:
        print("Done!")
        # close_session(session_metadata, host, headers)


def close_session(session_metadata, host, headers):
    print("Closing session....")
    session_id = session_metadata.json()['id']
    requests.delete(host + '/sessions/' + str(session_id), headers=headers)


def tf_worker_dummy_logic(tf_worker_id, json_file, total_tf_workers, epoch_id, host, headers, base_epoch_path, statements_url):
    json_file_path = base_epoch_path + "/" + str(epoch_id) + "/" + json_file
    data = []
    for line in open(json_file_path, 'r'):
        data.append(json.loads(line))
    if len(data) != 0:
        print ("Done consuming data for TF WORKER ID {}".format(tf_worker_id))
    notify_spark = {
        'code': textwrap.dedent("""
            if (totalWorkersNotified.incrementAndGet() == %s) {
                println("Done with all tf workers training for epochId %s!")
                import scala.reflect.io.Directory
                import java.io.File
                
                val epochDir = new Directory(new File("%s"))
                println(s"About to delete ${epochDir}")
                epochDir.deleteRecursively()
                println(s"Deleted ${epochDir}")
            }
        """ % (str(total_tf_workers), str(epoch_id), str(base_epoch_path + "/" + str(epoch_id))))
    }
    execute_spark(notify_spark, host, statements_url, headers)
    return True


def create_spark_session(host, spark_master, headers):
    print("Using host: " + host + ", and spark.master: " + spark_master)
    session_create_payload = {"conf":
        {
            "spark.master": spark_master,
            "spark.yarn.security.credentials.hive.enabled": "true",
        },
        "driverMemory": "6G",
        "executorCores": 2,
        "numExecutors": 5,
        "executorMemory": "2G",
        "queue": "default",
        "kind": "spark"
    }

    session_create_resp = requests.post(host + '/sessions', data=json.dumps(session_create_payload), headers=headers)
    return session_create_resp


def execute_spark(code, host, statements_url, headers):
    # print("statements_url: " + statements_url)
    # print(json.dumps(code))
    # print(headers)
    code_resp = requests.post(statements_url, data=json.dumps(code), headers=headers)
    print(code_resp.json())
    if code_resp.status_code == 201:
        code_result_url = host + code_resp.headers['location']
        while True:
            code_result = requests.get(code_result_url, headers=headers)
            if code_result.json()['state'] != "available":  ## means statement has an available result
                print("Still running...")
                time.sleep(5)
            else:
                pprint.pprint(code_result.json())
                break
    else:
        raise Exception("Something went wrong: " + str(code_resp.status_code) + ", Message: " + code_resp.reason)


if __name__ == '__main__':
    main()
