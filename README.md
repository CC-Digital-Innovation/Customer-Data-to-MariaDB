# Customer Data to MariaDB

## Summary
Gathers all ServiceNow tickets and Opsgenie alerts from configured instances
and pushes the data to a configured MariaDB database. It will seed empty
tables of the data, updates non-closed tickets / alerts, and pushes any new
data that is missing from the database every time the script is run.

_Note: If you have any questions or comments you can always use GitHub
discussions, or email me at farinaanthony96@gmail.com._

#### Why
Keeping a historical record of tickets and alerts for customers allows us to
make powerful dashboards and interpret the data not only for our customers, but
also for ourselves. We can get much more detailed metrics on our business and
our customer's businesses.

## Requirements
- Python 3.12+
- loguru
- mariadb
- pysnow
- python-dotenv
- python-magic-bin (only needed if running on a Windows system)
- pytz
- requests

## Usage
- Edit the example environment variables file with relevant Opsgenie,
  ServiceNow, and MariaDB information.

- Simply run the script using Python:
  `python customer_data_to_mariadb.py`

## Compatibility
Should be able to run on any machine with a Python interpreter. This script
was only tested on a Windows machine running Python 3.12.2.

## Disclaimer
The code provided in this project is an open source example and should not
be treated as an officially supported product. Use at your own risk. If you
encounter any problems, please log an
[issue](https://github.com/CC-Digital-Innovation/Customer-Data-to-MariaDB/issues).

## Contributing
1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request ツ

## History
-  version 1.0.0 - 2024/12/03
    - initial release

## Credits
Anthony Farina <<farinaanthony96@gmail.com>>