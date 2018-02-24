# White Swan Ankylosing Spondilytis 

## Overview


## Getting Started

### Checkout this repo
Clone this repo locally
```bash
git checkout ...
```

### Create a virtual env

```bash
conda create -n as-analysis python=3
source activate as-analysis
```

### Install required libraries
```bash
pip install -r requirements.txt
```


### Run the startup scripts
Download the data from the AWS servers and process them into a more useable format. 

```bash
python setup.py
```
