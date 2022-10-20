
# python-high-volume-address-validation-library


## Introduction 

This program is a wrapper around [Address Validation API](https://developers.google.com/maps/documentation/address-validation) enabling it to be processed in high volume which can be useful for many scenarios. 

The program takes a `csv` file. It then uses the API key configured in config.yaml to start the processing of the addresses. An example of config.yaml is as below:

```
# Location of the address file
address_file : './tests/addresses.csv'  
# Provide the columns which needs to be concatinated to get the address
column_numbers : [3,4,5,6,7]  
# Shelve db file
shelve_db : 'addresses'  
# Set the separator in the file with which to create the final address
separator : ","  
```
The `csv` file can have either the address in one single column or it can be split across multiple columns like [house number, street name, zipcode, city, state]. This can be configured through the config file.

## Flow

* Reads `csv` file
* Constructs the address as per configuration
* Stores the formatted addresses in a `shelve` object. This is done to make the program more resilient and async.
* It then picks up addresses one by one from the `shelve` object and call the Address Validation API
* Get the response back, parse it and store configured values back to the `shelve` object
* After all the addresses are inserted back to the datastructure, another piece of code executes and exports the data in a `csv` file
* This `csv` file is validated through a function which checks for [Google Maps Platform Terms of Service](https://cloud.google.com/maps-platform/terms) compliance
* Once the program is executed, it stores the [geocode](https://developers.google.com/maps/documentation/address-validation/requests-validate-address#response) and [`place ID`](https://developers.google.com/maps/documentation/places/web-service/place-id) against each given address and exports it in a `csv` file.


## Install and run 

* Requires `python3` and `PyYAML`:
  
  `brew install python3`  
  `brew install PyYAML`
  


* Install: python-high-volume-address-validation-library software also requires to have [google-maps-services-python](https://github.com/googlemaps/google-maps-services-python) installed, the latest version that includes Address Validation API:
  `
  pip3 install googlemaps
  `

* Update `config.yaml` file in `/src` folder with your API key, `csv` output path, and mode in which to run the library (see "Running Modes" section):
  ```
  ## Address Validation API key
  api_key : 'YOUR_API_KEY'

  ## Name of the output csv file
  output_csv : './test-results.csv'
  
  ## There are three modes for running the software.
  run_mode : 1
  ```

* Run:  
  `
  python3 main.py
  `

## Code Structure

  First the program will read the configuration located in `config.yaml`. `config.yaml` has the location of the address file which by default is `addresses.csv`.

  `main.py` will orchestrate across other functions and read the 
  `csv` file line by line and insert into a persistant Datastructure
  called `address` (by default) which is of type [Shelve](https://docs.python.org/3/library/shelve.html) 

  Then a second function will pick up the addresses in the shelve object and call the [Address 
  Validation API](https://developers.google.com/maps/documentation/address-validation)

  The response from the Address Validation API will be stored back in the `shelve`  Object. 
  Finally a `csv` file will be exported from the `shelve` object.

  The software works in three modes. You can set the mode to comply with [Google Maps Platform Terms of Service](https://cloud.google.com/maps-platform/terms), by configuring the `config.yaml` file corresponding to the use case under which this is run.


## Running Modes
  
  There are several running modes for the software. Running modes are essentially different scenarios or use cases under which the software can be run:

  1. ### Test Mode : 1  
      In test mode you are allowed to store more details from the Address Validation API response (this can be configured from `main.py` in variable `header`).

  2. ### Production mode -NoUsers : 2 (default)

      a Production mode <ins>not</ins> initiated after user/human interaction, only minimal data elements are allowed to be stored as per [Google Maps Platform Terms of Service](https://cloud.google.com/maps-platform/terms). Typically involves successive and multiple programmatic requests to Address Validation API. 

  3. ### Production mode -Users : 3

      a Production mode initiated after user/human interaction, some more data may be cached for the unique purpose of the user completing his singular task.

* Update the mode in `config.yaml` file inside `/src` folder :
  ```
  run_mode : 2
  ```