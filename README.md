# task_mlflow_wrapper

## install 

```
pip install git+https://github.com/YukiSakamoto/task_mlflow_wrapper.git
```

## Example code

```
import prefect
from prefect import flow
from task_mlflow_wrapper import task_with_mlflow

@task_with_mlflow()
def str_twice(s):
    return s*2

@task_with_mlflow()
def str_triple(s):
    return s*3

@task_with_mlflow()
def str_add(s1, s2):
    return s1+s2

@task_with_mlflow()
def str_and(s1,s2):
    return s1 and s2

@flow()
def my_test(a = "hello", b = "byebye"):
    a = str_twice(a)
    aa = str_twice(a)
    bb = str_twice(b)
    ret = str_add(aa,bb)
    print(ret)
    ret2 = str_and(aa, bb)
    print(ret2)
    return ret

if __name__ == '__main__':
    my_test()
```
