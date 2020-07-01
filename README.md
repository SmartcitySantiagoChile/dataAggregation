[![Build Status](https://travis-ci.com/SmartcitySantiagoChile/dataAggregation.svg?branch=master)](https://travis-ci.com/SmartcitySantiagoChile/dataAggregation) [![Coverage Status](https://coveralls.io/repos/github/SmartcitySantiagoChile/dataAggregation/badge.svg?branch=master)](https://coveralls.io/github/SmartcitySantiagoChile/dataAggregation?branch=master)
# dataAggregation

To create csv files with bip! transactions for each day.

## Requirements

- Python 3
- Dependencies: requirements.txt

### Install  project


Command to get project:

```
git clone https://github.com/SmartcitySantiagoChile/dataAggregation
```

You can download from github directly too: [https://github.com/](https://github.com/SmartcitySantiagoChile/dataAggregation/releases).

It's recommended to use a virtual environment to keep dependencies required by different projects separate by creating isolated python virtual environments for them.

To create a virtual environment:

```
virtualenv venv
```
If you are using Python 2.7 by default is needed to define a Python3 flag:

```
virtualenv -p python3 venv
```

Activate virtual env and install dependencies:
```
source venv/bin/activate
 
pip install -r requirements.txt
```


### .env file
The env files allow you to put your environment variables inside a file,
 it is recommended to only have to worry once about the setup and configuration of application
 and to not store passwords and sensitive data in public repository.
 
You need to define the environment keys creating an .env file at root path:
```
AWS_ACCESS_KEY_ID='PUT_HERE_YOUR_ACCESS_KEY'
AWS_SECRET_ACCESS_KEY='PUT_HERE_YOUR_SECRET_ACCESS_KEY'
MISCELLANEOUS_BUCKET_NAME='PUT_HERE_YOUR_MISCELLANEOUS_BUCKET_NAME'
```

MISCELLANEOUS_BUCKET_NAME is the name for aws bucket.

## Run tests
To verify that everything works well on your computer you can run these automatic tests that will tell you if there is a problem:

```
python -m unittest
```

## Usage   
## General Data
To run process_general_data you need to execute:

```
python process_general_data.py [path] [--output OUTPUT] [--send-to-s3] [--lower bound LOWER-BOUND] [--upper bound UPPER-BOUND]
```
- [path] path with files.
- [--output OUTPUT] output file path.
- [--send-to-s3]  send file to S3 bucket
- [--lower-bound LOWER-BOUND] lower-bound date in YY-MM-DD format
- [--upper bound UPPER-BOUND] upper-bound date in YY-MM-DD format


The output file will be a csv file saved at choosen output path or dataAggregation/output by default
### Help

To get help with command you need to execute:

```
python process_general_data.py -h 
```


## Trip Data
To run dataAggregation you need to execute:

```
python process_trip_data.py [path] [--output OUTPUT] [--send-to-s3] [--lower bound LOWER-BOUND] [--upper bound UPPER-BOUND]
```
- [path] path with files.
- [--output OUTPUT] output file path.
- [--send-to-s3]  send file to S3 bucket
- [--lower-bound LOWER-BOUND] lower-bound date in YY-MM-DD format
- [--upper bound UPPER-BOUND] upper-bound date in YY-MM-DD format


The output file will be a csv file saved at choosen output path or dataAggregation/output by default
### Help

To get help with command you need to execute:

```
python process_trip_data.py -h 
```
