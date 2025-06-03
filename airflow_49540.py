from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import task

from airflow.models.dag import DAG


log = logging.getLogger(__name__)


with DAG(
    "xcom_test",
    schedule="* * * * *",  # Override to match your needs,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "sheets"],
) as dag:
  @task
  def xcom_string():
    return "string"

  @task
  def xcom_dict():
    return {"a": "b"}


  @task
  def xcom_list():
    return [("a", "b"), ("c", "d")]


  @task
  def read_xcom(**kwargs):
    xcom_string_task_value = kwargs["task_instance"].xcom_pull("xcom_string")
    print("xcom_string_task", type(xcom_string_task_value), xcom_string_task_value)

    xcom_string_task_value_ids = kwargs["task_instance"].xcom_pull(task_ids=["xcom_string"], key="return_value")
    print("xcom_string_task_ids", type(xcom_string_task_value_ids), xcom_string_task_value_ids, xcom_string_task_value_ids[0])

    xcom_dict_task_value = kwargs["task_instance"].xcom_pull("xcom_dict")
    print("xcom_dict_task", type(xcom_dict_task_value), xcom_dict_task_value)

    xcom_dict_task_value_ids = kwargs["task_instance"].xcom_pull(task_ids=["xcom_dict"], key="return_value")
    print("xcom_dict_task_ids", type(xcom_dict_task_value_ids), xcom_dict_task_value_ids, xcom_dict_task_value_ids[0])

    xcom_list_task_value = kwargs["task_instance"].xcom_pull("xcom_list")
    print("xcom_list_task", type(xcom_list_task_value), xcom_list_task_value)

    xcom_list_task_value_ids = kwargs["task_instance"].xcom_pull(task_ids=["xcom_list"], key="return_value")
    print("xcom_list_task_ids", type(xcom_list_task_value_ids), xcom_list_task_value_ids, xcom_list_task_value_ids[0])


  xcom_string = xcom_string()
  xcom_dict = xcom_dict()
  xcom_list = xcom_list()

  read_xcom_task = read_xcom()
  xcom_string >> xcom_dict >> xcom_list >> read_xcom_task