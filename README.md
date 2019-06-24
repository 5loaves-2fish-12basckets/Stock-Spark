# Stock Spark

2019 Spring Cloud Computing Final Project

## introduction

This repository is the final project made in cooperation by 葉津源 and 陳冠甫.
We constructed a simple system integrating various apis to:
* download realtime stock data (with alphavintage)
* train and run tensorflow model prediction on stock trend (with sparkflow)
* create simple backend server and frontend visualization (with python, firebase and react)


## repository structure

### doc
contains the final presentation and report 
*   [final report](doc/Final_Project_Report.pdf) 
*   [presentation](doc/presentation.pdf)  

### src
contains code for training, backend and frontend script
* [`datafunc.py`](src/datafunc.py) is for calling alpha_vantage api and downloading csv files
* [`model.py`](src/model.py) contains a small tensorflow model script
* [`train.py`](src/train.py) is the main training algorithm

#### [frontend](src/frontend)
[frontend](src/frontend) contains frontend specifics, check the [README](src/frontend/README.md) for further introduction

## usage

`bash train.sh <dir>` to train and save new model in `<dir>`      
`bash run.sh <dir>` to start up backend server and use model in `<dir>`    
  
(`<dir>` is optional, default: ckpt)  

## requirements

python3
pyspark
tensorflow
tqdm

alpha_vantage
sparkflow

## references
[alpha_vantage](https://www.alphavantage.co/)
[sparkflow](https://github.com/lifeomic/sparkflow)