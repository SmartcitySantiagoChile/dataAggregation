[![Build Status](https://travis-ci.com/SmartcitySantiagoChile/TransactionByStopVis.svg?branch=master)](https://travis-ci.com/SmartcitySantiagoChile/TransactionByStopVis)
[![Coverage Status](https://coveralls.io/repos/github/SmartcitySantiagoChile/TransactionByStopVis/badge.svg)](https://coveralls.io/github/SmartcitySantiagoChile/TransactionByStopVis)

# TransactionByStopVis

To create a visualization with bip! transactions for each stop (bus or metro) per day.

## Requirements

- Python 3
- Dependencies: requirements.txt
- stops.csv 
### Install Python 3
#### Windows users

You need to download  Python 3 from official web site:
 
 https://www.python.org/downloads/windows/

Select version and download either <strong> Windows x86-64 executable installer </strong> or <strong> Windows x86 executable installer</strong>.

Run Python Installer once downloaded. (In this example, we have downloaded Python 3.8.3). 

![Tutorial](readme_data/windows.png)


Make sure you select the <strong>Install launcher for all users </strong> and <strong> Add Python 3.8 </strong> to PATH checkboxes. The latter places the interpreter in the execution path.

![Tutorial](readme_data/windows-2.png   )

After setup was successful check the <strong>Disable path length limit </strong> option. Choosing this option
 will allow Python to bypass 260-character MAX_PATH limit. It is recommended to resolve potential lenght
  issues that may arise with Python projects developed in Linux (Like this project.) 

![Tutorial](readme_data/windows-3.png   )

### Install  project

First clone repository at your computer, for instance you can use path `c:\project` in windows or `/home/user/project` for linux environments. If folder does not exist, you have to create it.

Command to get project:

```
git clone https://github.com/SmartcitySantiagoChile/TransactionByStopVis
```

You can download from github directly too: [https://github.com/](https://github.com/SmartcitySantiagoChile/TransactionByStopVis/releases).

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
EARLY_TRANSACTION_BUCKET_NAME='PUT_HERE_YOUR_TRANSACTION_BUCKET_NAME'
MAPBOX_KEY='PUT_HERE_YOUR_MAPBOX_KEY'
```

PUT_HERE_YOUR_ACCESS_KEY y PUT_HERE_YOUR_SECRET_ACCESS_KEY can be obtained by an AWS user credentials (https://console.aws.amazon.com/iam/home?#/users).

EARLY_TRANSACTION_BUCKET_NAME is the name for aws bucket.

MAPBOX_KEY can be obtained by a Mapbox Account (https://docs.mapbox.com/help/how-mapbox-works/access-tokens/)

## Run tests
To verify that everything works well on your computer you can run these automatic tests that will tell you if there is a problem:

```
python -m unittest
```

## Usage    

To run TransactionByStopVis you need to execute:


python process_data.py [start_date] [end_date] [output_name]

```
- [output_name] html file name
- [start_date] start date in YY-MM-DD format.
- [end_date]  end date in YY-MM-DD format.
```



The output file will be a html file saved at outputs path. 
## Help

To get help with command you need to execute:

```
python process_data.py -h 
```

## Tutorial

#### Executing command

First we're going go to execute process_data.py between 2020-05-08 and 2020-05-12


```
python process_data.py 2020-05-08  2020-05-12 tutorial
```
If all runs successfully we get the output:
```
> tutorial successfully created!
```

The output can be opened with a web navigator and looks like:

![Tutorial](readme_data/tutorial-1.png)

If you want to show legend info, you should click de upper right icon:

![Tutorial](readme_data/tutorial-2.png)

![Tutorial](readme_data/tutorial-3.png)

If you click on stops it will show info:

![Tutorial](readme_data/tutorial-4.png)

You can show Metro stops clicking on the left column (eye icon):

![Tutorial](readme_data/tutorial-5.png)
![Tutorial](readme_data/tutorial-6.png)

You can use timeline widget to see changes through time:

![Tutorial](readme_data/tutorial-7.png)




## FAQ

### Where do we get stop file?

It is one of inputs for ADATRAP vis, it represents a set of valid stops for period of time (operational program duration)
