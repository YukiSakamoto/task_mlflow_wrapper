import prefect
from prefect import task, flow, get_run_logger
from prefect.artifacts import create_link_artifact
import mlflow
import datetime
import functools
import inspect
import json
from pathlib import PosixPath, Path

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, PosixPath):
            return str(obj)
        return super().default(obj)

#========================================
#   Settings of the mlflow server URL
#========================================
_mlflow_server_uri = "127.0.0.1"
_mlflow_server_port = "7777"

def set_mlflow_server_uri(server_uri):
    global _mlflow_server_uri
    _mlflow_server_uri = server_uri

def set_mlflow_server_port(server_port):
    global _mlflow_server_port
    _mlflow_server_port = server_port

def get_mlflow_server_uri():
    return _mlflow_server_uri

def get_mlflow_server_port():
    return _mlflow_server_port

def _mlflow_endpoint():
    return "{}:{}".format(_mlflow_server_uri, _mlflow_server_port)

def task_with_mlflow(mlflow_server_uri = None, artifact_dir = None, 
        arg_name_artifact_dir_before_exec = None, arg_name_artifact_dir_after_exec = None,
        pathobj_log_artifacts = False):
    def prefect_task_wrapper(mlflow_server_endpoint, exp_id, run_id):
        def prefect_task_wrapper2(f):
            # This wrapper function is for the logging of the mlflow url.
            @task
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                experiment_url = f"http://{mlflow_server_endpoint}/#/experiments/{exp_id}/runs/{run_id}"
                description = "{} exp_id: {} run_id: {}".format(f.__name__, exp_id, run_id)
                create_link_artifact(key = "mlflow", link = experiment_url, description = description)
                ret = f(*args, **kwargs) 
                return ret
            return wrapper
        return prefect_task_wrapper2

    def task_with_mlflow_wrapper(f):
        def _helper_pathobj_log_artifacts(arg_dict: dict, artifact_uri_base: str):
            # This function save Path, or list of Path object as artifacts.
            for k, v in arg_dict.items():
                if isinstance(v, Path):
                    mlflow.log_artifacts(v, "{}/{}".format(artifact_uri_base, k))
                elif isinstance(v, list) or isinstance(v, set):
                    for index, v_i in enumerate(v):
                        if isinstance(v_i, Path):
                            mlflow.log_artifacts(v_i, "{}/{}/{}".format(artifact_uri_base, k, index))
                else:
                    pass

        @functools.wraps(f)
        def _wrapper(*args, **kwargs):
            #----------------------------------------
            # Get Function Name and Arguments
            #----------------------------------------
            function_name = f.__name__
            signature = inspect.signature(f)
            param_names = [param.name for param in signature.parameters.values()]
            bound_args = signature.bind(*args, **kwargs)
            bound_args.apply_defaults()
            #----------------------------------------
            # Set up MLFlow
            #----------------------------------------
            mlflow.set_experiment(function_name)
            with mlflow.start_run() as run:
                run_id = run.info.run_id
                experiment_id = run.info.experiment_id
                #----------------------------------------
                # MLFlow: Save the input parameters 
                #----------------------------------------
                for name, value in bound_args.arguments.items():
                    mlflow.log_param(name, value)

                #----------------------------------------
                # MLFlow: Save the Artifacts before exec. 
                #----------------------------------------
                if arg_name_artifact_dir_before_exec != None:
                    if arg_name_artifact_dir_before_exec in bound_args.arguments:
                        artifact_dir = bound_args.arguments[arg_name_artifact_dir_before_exec]
                        if artifact_dir != None:
                            if isinstance(artifact_dir, list) or isinstance(artifact_dir, set):
                                for index, d in enumerate(artifact_dir):
                                    mlflow.log_artifacts(str(d), artifact_path = "artifacts_before_exec/{}".format(index))
                            else:
                                mlflow.log_artifacts(str(artifact_dir), artifact_path = "artifacts_before_exec")

                #----------------------------------------
                # Execution
                #----------------------------------------
                decorate_func = prefect_task_wrapper(mlflow_server_endpoint = _mlflow_endpoint(), exp_id = experiment_id, run_id = run_id)(f)
                ret_value = decorate_func(*args, **kwargs)
                #----------------------------------------
                # Prefect: Save the log 
                #----------------------------------------
                experiment_url = f"http://{_mlflow_endpoint()}/#/experiments/{experiment_id}/runs/{run_id}"
                logger = get_run_logger()
                logger.info(f"{function_name}: {_mlflow_endpoint()}")
                #----------------------------------------
                # MLFlow: save all artifact
                #----------------------------------------
                if arg_name_artifact_dir_after_exec != None:
                    if arg_name_artifact_dir_after_exec in bound_args.arguments:
                        artifact_dir = bound_args.arguments[arg_name_artifact_dir_after_exec]
                        if artifact_dir != None:
                            if isinstance(artifact_dir, list) or isinstance(artifact_dir, set):
                                for index, d in enumerate(artifact_dir):
                                    mlflow.log_artifacts(str(artifact_dir), artifact_path = "artifacts_after_exec/{}".format(index))
                            else:
                                mlflow.log_artifacts(str(artifact_dir), artifact_path = "artifacts_after_exec")

                if pathobj_log_artifacts == True:
                    artifact_uri_base = "pathobj_artifacts"
                    _helper_pathobj_log_artifacts(bound_args.arguments, artifact_uri_base)

                #----------------------------------------
                # MLFlow: save inputs parameter and return value
                #----------------------------------------
                task_desc = dict()
                task_desc["inputs"] = bound_args.arguments
                task_desc["output"] = ret_value
                json_obj = json.dumps(task_desc, cls=CustomEncoder)
                mlflow.log_dict(json.loads(json_obj), "log.json")
                
                if isinstance(ret_value, float):
                    mlflow.log_metric(function_name, ret_value)
            return ret_value 
        return _wrapper

    return task_with_mlflow_wrapper

#@task_with_mlflow(arg_name_artifact_dir_before_exec = "artifact_dir", arg_name_artifact_dir_after_exec = "artifact_dir")
#def str_twice(s, artifact_dir = None):
#    return s*2
#
#
#@task_with_mlflow()
#def str_triple(s):
#    return s*3
#
#@task_with_mlflow()
#def str_add(s1, s2):
#    #create_link_artifact(link = "www.google.com", key = "yahoo", description = "yahoo description")
#    return s1+s2
#
#@task_with_mlflow()
#def str_and(s1,s2):
#    return s1 and s2
#
#@flow()
#def my_test(a = "hello", b = "byebye"):
#    a = str_twice(a, artifact_dir = "./hoge")
#    aa = str_twice(a)
#    bb = str_twice(b)
#    ret = str_add(aa,bb)
#    print(ret)

if __name__ == '__main__':
    #my_test()
    pass
