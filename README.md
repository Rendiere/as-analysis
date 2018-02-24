# White Swan Ankylosing Spondilytis 

## Overview
This project deals with the analysis of data of patients with Ankylosing Spondilytis. 

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

### Inspect Jupyter Notebooks
Lunch Jupyter and go through the notebooks

```bash
jupyter notebook
```
