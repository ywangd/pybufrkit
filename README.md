# Python Toolkit for WMO BUFR Messages

**Pure** Python package to decode and encode WMO BUFR (FM-94) messages. It can be used
as both a command line tool or library to decode and encode BUFR messages.


## Installation
Install from the source code 
```
python setup.py install
```
Or from PyPI
```
pip install pybufrkit
```


## Usage
The command line usage of the toolkit takes the form of 
```
pybufrkit [OPTIONS] sub-command ...
```
where the `sub-command` is an action, e.g. decode, encode, that can be performed by the tool. 
To see a full list of available sub-command, run `pybufrkit -h`. Here are a few example usages:


* Decode a BUFR file
    - `pybufrkit decode BUFR_FILE`

* Decode a BUFR file and convert to JSON format
    - `pybufrkit decode -j BUFR_FILE`
    
* Show only the metadata sections of a BUFR file
    - `pybufrkit info BUFR_FILE`
    
* Encode from a JSON file to BUFR
    - `pybufrkit encode JSON_FILE BUFR_FILE`
    - `pybufrkit decode -j BUFR_FILE | pybufrkit encode -`
    
* Lookup information for a descriptor
    - `pybufrkit lookup 309052`
    - `pybufrkit lookup -l 020003`
    

    


